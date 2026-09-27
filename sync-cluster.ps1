<#
sync-cluster.ps1 -- moves the code from this laptop (Windows) to a cluster
master, and the results back. Run it in PowerShell from the my_scripts folder.
It uses the ssh and tar that come with Windows 10/11 and asks for the password
once per call unless an ssh key is installed.

USAGE
  .\sync-cluster.ps1 push [user@host]
      Makes ~/my_scripts on the host match this folder: every file git knows
      about (committed or not, new files too; ignored files are skipped), with
      Linux line endings, plus a VERSION file with the commit id. Files deleted
      here since the last push are deleted there; results/ is never touched.
      Refuses while an experiment is running on the host.

  .\sync-cluster.ps1 pull [user@host] ['patterns']
      Copies ~/my_scripts/results/<patterns> (default: 'pipeline_2*
      storage_virtualization_loopback_* storage_bench_*') into the folder that
      holds my_scripts (hadoop\), then writes FINAL_REPORT.md with the figures
      for the newest pipeline_* folder it copied. To pull from another folder
      on the host, first set:  $env:REMOTE_RESULTS = '/scratch/results'

  user@host defaults to mostufa.j@tapuz14.cslcs.technion.ac.il.
  CloudLab example: .\sync-cluster.ps1 push Mostufa@er101.utah.cloudlab.us
#>
param(
    [Parameter(Position = 0)][string]$Action = '',
    [Parameter(Position = 1)][string]$Target = 'mostufa.j@tapuz14.cslcs.technion.ac.il',
    [Parameter(Position = 2)][string]$Patterns = 'pipeline_2* storage_virtualization_loopback_* storage_bench_*'
)

Set-StrictMode -Version 2
$ErrorActionPreference = 'Stop'

$RepoDir = $PSScriptRoot
$Tar = Join-Path $env:SystemRoot 'System32\tar.exe'   # bsdtar; understands C:\ paths
$Utf8 = New-Object System.Text.UTF8Encoding($false)

# Runs on the host (bash) with the archive on stdin.
$PushScript = @'
set -eu
export LC_ALL=C
dest="$HOME/my_scripts"
busy='bash .*(run-all|run-2x2|run-experiment-loopback-fs|storage-bench)\.sh'
if command -v pgrep >/dev/null 2>&1 && pgrep -u "$(id -u)" -f "$busy" >/dev/null; then
    {
        echo "An experiment is running on $(hostname):"
        pgrep -u "$(id -u)" -af "$busy" | sed 's/^/  /' || true
        echo "Pushing now would change its scripts halfway through. Wait until it"
        echo "finishes, or stop it first (screen -r exp, Ctrl+C, then"
        echo "bash stop-single-dn-cluster.sh 1024)."
    } >&2
    exit 3
fi
mkdir -p "$dest"
cd "$dest"
old=$(cat .sync-manifest 2>/dev/null || true)
tar -xzf -
# Files the previous push sent that are gone from the laptop now.
removed=0
if [ -n "$old" ]; then
    gone=$(comm -23 <(printf '%s\n' "$old" | sort -u) <(sort -u .sync-manifest))
    while IFS= read -r f; do
        case "$f" in '' | /* | *..*) continue ;; esac
        rm -f -- "$f"
        rmdir -p --ignore-fail-on-non-empty -- "$(dirname -- "$f")" 2>/dev/null || true
        removed=$((removed + 1))
    done <<<"$gone"
fi
# Windows has no execute bit: give it back to the scripts (#! on line 1).
while IFS= read -r f; do
    if [ -n "$f" ] && [ -f "$f" ]; then chmod +x -- "$f"; fi
done <.sync-exec
echo "  $(grep -c . .sync-manifest) files in $dest, version $(cut -d' ' -f1 VERSION)"
if [ "$removed" -gt 0 ]; then echo "  removed $removed file(s) that were deleted on the laptop"; fi
'@

# Runs on the host (bash); writes the archive to stdout, messages to stderr.
$PullScript = @'
set -eu
dir=$1
shift
case "$dir" in
    \~) dir=$HOME ;;
    \~/*) dir="$HOME/${dir#\~/}" ;;
esac
if ! cd "$dir" 2>/dev/null; then
    echo "No folder $dir on $(hostname)." >&2
    exit 4
fi
shopt -s nullglob
items=()
for p in "$@"; do
    for f in $p; do items+=("$f"); done
done
if [ "${#items[@]}" -eq 0 ]; then
    echo "Nothing in $dir on $(hostname) matches: $*" >&2
    exit 5
fi
echo "  packing on $(hostname): ${items[*]}" >&2
rc=0
tar -czf - --warning=no-file-changed --exclude=latest --exclude=pipeline_latest "${items[@]}" || rc=$?
if [ "$rc" -eq 1 ]; then
    echo "  (some files were still being written; they are copied as they were)" >&2
    rc=0
fi
exit "$rc"
'@

function Show-Usage {
    $lines = @(Get-Content -LiteralPath $PSCommandPath)
    $from = [array]::IndexOf($lines, 'USAGE')
    $to = [array]::IndexOf($lines, '#>')
    $lines[$from..($to - 1)] | ForEach-Object { Write-Host $_ }
}

function New-WorkDir {
    $dir = Join-Path ([System.IO.Path]::GetTempPath()) ('sync-cluster-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
    [void](New-Item -ItemType Directory -Path $dir)
    return $dir
}

# Runs a bash script on the host. The script travels base64-encoded inside the
# ssh command, so neither PowerShell nor the login shell there (bash or tcsh)
# can mangle its quotes. Start-Process redirects stdin/stdout straight to
# files, which (unlike a PowerShell 5 pipe) keeps binary data intact.
function Invoke-Remote([string]$Script, [string[]]$Arguments = @(), [string]$StdIn = '', [string]$StdOut = '') {
    $b64 = [Convert]::ToBase64String($Utf8.GetBytes($Script.Replace("`r", '')))
    $quoted = (@($Arguments) | ForEach-Object { "'$_'" }) -join ' '
    $remote = "bash -c 'eval `"`$(echo $b64 | base64 -d)`"' sync-cluster $quoted"
    $start = @{
        FilePath     = $Ssh
        ArgumentList = $Target + ' "' + $remote.Replace('"', '\"') + '"'
        NoNewWindow  = $true
        Wait         = $true
        PassThru     = $true
    }
    if ($StdIn) { $start.RedirectStandardInput = $StdIn }
    if ($StdOut) { $start.RedirectStandardOutput = $StdOut }
    $proc = Start-Process @start
    return $proc.ExitCode
}

function Write-Failure([int]$Code, [string]$What) {
    if ($Code -eq 255) {
        Write-Host "ssh could not connect or log in to $Target (exit 255)." -ForegroundColor Red
    } else {
        Write-Host "$What failed (exit $Code); see the messages above." -ForegroundColor Red
    }
}

function Invoke-Push {
    $entries = @(& git -C $RepoDir -c core.quotepath=off ls-files --eol --cached --others --exclude-standard)
    if ($LASTEXITCODE -ne 0) { throw "git ls-files failed in $RepoDir" }
    $version = (& git -C $RepoDir rev-parse --short HEAD | Out-String).Trim()
    $changes = @(& git -C $RepoDir status --porcelain)
    if ($changes.Count -gt 0) { $version += '-dirty' }

    $work = New-WorkDir
    $stage = Join-Path $work 'files'
    try {
        $latin1 = [System.Text.Encoding]::GetEncoding(28591)   # byte-for-byte
        $files = New-Object System.Collections.Generic.List[string]
        $exec = New-Object System.Collections.Generic.List[string]
        $converted = New-Object System.Collections.Generic.List[string]
        foreach ($entry in ($entries | Sort-Object -Unique)) {
            $info, $rel = $entry -split "`t", 2
            $src = Join-Path $RepoDir $rel
            if (-not (Test-Path -LiteralPath $src -PathType Leaf)) { continue }   # deleted, not committed yet
            $bytes = [System.IO.File]::ReadAllBytes($src)
            if ($info -match 'w/(crlf|mixed)') {
                # CRLF breaks bash on Linux; send LF (git stores LF anyway).
                $bytes = $latin1.GetBytes($latin1.GetString($bytes).Replace("`r`n", "`n"))
                $converted.Add($rel)
            }
            $dst = Join-Path $stage $rel
            [void](New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dst))
            [System.IO.File]::WriteAllBytes($dst, $bytes)
            $files.Add($rel)
            if ($bytes.Length -ge 2 -and $bytes[0] -eq 0x23 -and $bytes[1] -eq 0x21) { $exec.Add($rel) }
        }
        [System.IO.File]::WriteAllText((Join-Path $stage '.sync-manifest'), (($files -join "`n") + "`n"), $Utf8)
        [System.IO.File]::WriteAllText((Join-Path $stage '.sync-exec'), (($exec -join "`n") + "`n"), $Utf8)
        $stamp = Get-Date -Format "yyyy-MM-dd'T'HH:mm:sszzz"
        [System.IO.File]::WriteAllText((Join-Path $stage 'VERSION'), "$version $stamp`n", $Utf8)
        $list = Join-Path $work 'list.txt'
        $names = @($files) + @('VERSION', '.sync-manifest', '.sync-exec')
        [System.IO.File]::WriteAllText($list, (($names -join "`n") + "`n"), $Utf8)
        # Written to a file, not stdout: bsdtar pads piped output, and gzip on
        # the host would call the padding an error.
        $tgz = Join-Path $work 'push.tgz'
        & $Tar -czf $tgz -C $stage -T $list
        if ($LASTEXITCODE -ne 0) { throw 'tar could not pack the files' }

        if ($changes.Count -gt 0) { Write-Host "Note: uncommitted changes are included (version $version)." }
        if ($converted.Count -gt 0) {
            Write-Host "Sent with Linux line endings (they have Windows ones here): $($converted -join ', ')"
        }
        $mb = (Get-Item -LiteralPath $tgz).Length / 1MB
        Write-Host ('Pushing {0} files ({1:N1} MB) to {2}:~/my_scripts ...' -f $files.Count, $mb, $Target)
        $rc = Invoke-Remote -Script $PushScript -StdIn $tgz
    } finally {
        Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
    }
    if ($rc -ne 0) { Write-Failure $rc 'push'; exit $rc }
    Write-Host 'Done.'
}

function Write-Report([string]$PipeDir) {
    $reportPy = Join-Path $RepoDir 'experiments\storage_virtualization_loopback\final-report.py'
    $exe = $null
    $pre = @()
    # "python" in WindowsApps is only the Microsoft Store placeholder.
    $py = Get-Command python -CommandType Application -ErrorAction SilentlyContinue |
        Where-Object { $_.Source -notlike '*\WindowsApps\*' } | Select-Object -First 1
    if ($py) {
        $exe = $py.Source
    } else {
        $py = Get-Command py -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($py) { $exe = $py.Source; $pre = @('-3') }
    }
    if (-not $exe) {
        Write-Host 'Python not found, so no report yet. Install Python with matplotlib, then run:'
        Write-Host "  python `"$reportPy`" `"$PipeDir`""
        return
    }
    Write-Host "Writing FINAL_REPORT.md and the figures for $(Split-Path -Leaf $PipeDir) ..."
    $saved = $env:PYTHONIOENCODING
    $env:PYTHONIOENCODING = 'utf-8'
    try { & $exe @pre $reportPy $PipeDir } finally { $env:PYTHONIOENCODING = $saved }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "final-report.py failed (exit $LASTEXITCODE); the results themselves were copied." -ForegroundColor Yellow
    }
}

function Invoke-Pull {
    $patternList = @($Patterns -split '\s+' | Where-Object { $_ })
    $remoteDir = '~/my_scripts/results'
    if ($env:REMOTE_RESULTS) { $remoteDir = $env:REMOTE_RESULTS }
    foreach ($p in @($remoteDir) + $patternList) {
        if ($p -notmatch '^[A-Za-z0-9_.*?/~\[\]-]+$') { throw "Unsupported characters in '$p'." }
    }
    $dest = Split-Path -Parent $RepoDir
    $work = New-WorkDir
    $top = @()
    try {
        $tgz = Join-Path $work 'pull.tgz'
        Write-Host "Pulling ${Target}:$remoteDir/ ($($patternList -join ' ')) into $dest ..."
        $rc = Invoke-Remote -Script $PullScript -Arguments (@($remoteDir) + $patternList) -StdOut $tgz
        if ($rc -eq 0) {
            $top = @(& $Tar -tzf $tgz | ForEach-Object { ($_ -split '/')[0] } | Sort-Object -Unique)
            & $Tar -xzf $tgz -C $dest
            if ($LASTEXITCODE -ne 0) { throw "tar could not unpack into $dest" }
        }
    } finally {
        Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
    }
    if ($rc -ne 0) { Write-Failure $rc 'pull'; exit $rc }
    Write-Host "Copied into ${dest}: $($top -join ', ')"
    $pipes = @($top | Where-Object { $_ -like 'pipeline_2*' })
    if ($pipes.Count -gt 0) { Write-Report (Join-Path $dest $pipes[-1]) }
}

if ($Action -notin @('push', 'pull')) { Show-Usage; exit 1 }
if (-not (Test-Path -LiteralPath $Tar)) {
    Write-Host "$Tar not found (it comes with Windows 10 1803 and newer)." -ForegroundColor Red
    exit 1
}
$sshCmd = Get-Command ssh -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $sshCmd) {
    Write-Host 'ssh not found: add "OpenSSH Client" in Settings > System > Optional features.' -ForegroundColor Red
    exit 1
}
$Ssh = $sshCmd.Source
if ($Action -eq 'push') { Invoke-Push } else { Invoke-Pull }
exit 0
