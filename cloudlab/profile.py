#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
## Storage Virtualization Loopback Filesystem Experiment on CloudLab m400

This profile provisions **5 ARM m400 nodes on CloudLab** for the Hadoop storage virtualization experiment.

**Key features:**
- Bare metal m400 ARM nodes (8 cores, 64GB RAM, 120GB disk each)
- All available disk (~50GB) allocated to `/mydata` for experiment data
- Ubuntu 18.04 LTS with Java 11, Python 3, essentials pre-installed
- Full LAN connectivity for cluster communication

**Hardware constraints (m400):**
- Total disk: 120 GB (SATA SSD)
- Available for experiments: ~50 GB (after OS + system overhead)
- Default loopback budget: 30 GB per DataNode (reduced from 220 GB for larger clusters)

Instructions:

### 1. Wait for Boot (2-5 minutes)
After instantiation, `mkextrafs.pl` will create `/mydata` with all available disk.

### 2. SSH to Master Node
```bash
ssh <username>@node0.experiment.cloudlab.utahcloud.com
```

### 3. Clone the Repository
```bash
git clone <your-repo-url> ~/my_scripts
cd ~/my_scripts
```

### 4. **CRITICAL: Edit Loopback Budget**
For m400's 120 GB disk (~50 GB available), reduce the loopback budget:
```bash
nano experiments/storage_virtualization_loopback/run-experiment-loopback-fs.sh

# Find line ~50: LOOPBACK_BUDGET_PER_NODE_GB=220
# Change to: LOOPBACK_BUDGET_PER_NODE_GB=30

# Also reduce K_VALUES around line ~60:
# OLD: K_VALUES=(1 2 4 8 16 32 64 128 256 512 1024)
# NEW: K_VALUES=(1 2 4 8 16 32 64)
```

### 5. Create nodes.txt
```bash
cat > cloudlab/nodes.txt << EOF
node0
node1
node2
node3
node4
EOF
```

### 6. Bootstrap Nodes (from your workstation)
```bash
bash cloudlab/bootstrap.sh cloudlab/nodes.txt
```

### 7. Verify Setup on Master
```bash
df -h /scratch
/scratch/hadoop/hadoop-3.3.1/bin/hadoop version
```

### 8. Run the Experiment
```bash
bash ~/my_scripts/cloudlab/run-experiment-wrapper.sh 3
```

### 9. Monitor Progress
```bash
tail -f /scratch/results/storage_virtualization_loopback/latest/experiment.log
```

### 10. Retrieve Results
```bash
ls -lh /scratch/results/storage_virtualization_loopback/latest/
```

See `cloudlab/README.md` and `cloudlab/CLOUDLAB_CHECKLIST.md` in your repo for detailed setup and troubleshooting.
"""

import geni.portal as portal
import geni.rspec.pg as rspec

# Create a portal context.
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Variable number of nodes.
pc.defineParameter("nodeCount", "Number of Nodes", portal.ParameterType.INTEGER, 5,
                   longDescription="If you specify more then one node, " +
                   "we will create a lan for you.")

# Pick your OS.
imageList = [
    ('default', 'Default Image'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU24-64-STD', 'Ubuntu 24.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD', 'Ubuntu 22.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD', 'Ubuntu 20.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD', 'Ubuntu 18.04'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//CENTOS9S-64-STD', 'CentOS 9 Stream'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//CENTOS8S-64-STD', 'CentOS 8 Stream'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//ROCKY9-64-STD',   'Rocky Linux 9'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//FBSD135-64-STD',  'FreeBSD 13.5'),
    ('urn:publicid:IDN+emulab.net+image+emulab-ops//FBSD142-64-STD',  'FreeBSD 14.2')]

pc.defineParameter("osImage", "Select OS image",
                   portal.ParameterType.IMAGE,
                   imageList[4], imageList,
                   longDescription="Most clusters have this set of images, " +
                   "pick your favorite one.")

# Optional physical type for all nodes.
pc.defineParameter("phystype",  "Optional physical node type",
                   portal.ParameterType.NODETYPE, "m400",
                   longDescription="Pick a single physical node type (pc3000,d710,etc) " +
                   "instead of letting the resource mapper choose for you.")

# Optionally create XEN VMs instead of allocating bare metal nodes.
pc.defineParameter("useVMs",  "Use XEN VMs",
                   portal.ParameterType.BOOLEAN, False,
                   longDescription="Create XEN VMs instead of allocating bare metal nodes.")

# Optionally start X11 VNC server.
pc.defineParameter("startVNC",  "Start X11 VNC on your nodes",
                   portal.ParameterType.BOOLEAN, False,
                   longDescription="Start X11 VNC server on your nodes. There will be " +
                   "a menu option in the node context menu to start a browser based VNC " +
                   "client. Works really well, give it a try!")

# Optional link speed, normally the resource mapper will choose for you based on node availability
pc.defineParameter("linkSpeed", "Link Speed",portal.ParameterType.INTEGER, 0,
                   [(0,"Any"),(100000,"100Mb/s"),(1000000,"1Gb/s"),(10000000,"10Gb/s"),(25000000,"25Gb/s"),(100000000,"100Gb/s")],
                   advanced=True,
                   longDescription="A specific link speed to use for your lan. Normally the resource " +
                   "mapper will choose for you based on node availability and the optional physical type.")
                   
# For very large lans you might to tell the resource mapper to override the bandwidth constraints
# and treat it a "best-effort"
pc.defineParameter("bestEffort",  "Best Effort", portal.ParameterType.BOOLEAN, False,
                    advanced=True,
                    longDescription="For very large lans, you might get an error saying 'not enough bandwidth.' " +
                    "This options tells the resource mapper to ignore bandwidth and assume you know what you " +
                    "are doing, just give me the lan I ask for (if enough nodes are available).")
                    
# Sometimes you want all of nodes on the same switch, Note that this option can make it impossible
# for your experiment to map.
pc.defineParameter("sameSwitch",  "No Interswitch Links", portal.ParameterType.BOOLEAN, False,
                    advanced=True,
                    longDescription="Sometimes you want all the nodes connected to the same switch. " +
                    "This option will ask the resource mapper to do that, although it might make " +
                    "it imppossible to find a solution. Do not use this unless you are sure you need it!")

# Optional ephemeral blockstore
pc.defineParameter("tempFileSystemSize", "Temporary Filesystem Size",
                   portal.ParameterType.INTEGER, 0,advanced=True,
                   longDescription="The size in GB of a temporary file system to mount on each of your " +
                   "nodes. Temporary means that they are deleted when your experiment is terminated. " +
                   "The images provided by the system have small root partitions, so use this option " +
                   "if you expect you will need more space to build your software packages or store " +
                   "temporary files.")
                   
# Instead of a size, ask for all available space. 
pc.defineParameter("tempFileSystemMax",  "Temp Filesystem Max Space",
                    portal.ParameterType.BOOLEAN, True,
                    advanced=True,
                    longDescription="Instead of specifying a size for your temporary filesystem, " +
                    "check this box to allocate all available disk space. Leave the size above as zero.")

pc.defineParameter("tempFileSystemMount", "Temporary Filesystem Mount Point",
                   portal.ParameterType.STRING,"/mydata",advanced=True,
                   longDescription="Mount the temporary file system at this mount point; in general you " +
                   "you do not need to change this, but we provide the option just in case your software " +
                   "is finicky.")

pc.defineParameter("exclusiveVMs", "Force use of exclusive VMs",
                   portal.ParameterType.BOOLEAN, True,
                   advanced=True,
                   longDescription="When True and useVMs is specified, setting this will force allocation " +
                   "of exclusive VMs. When False, VMs may be shared or exclusive depending on the policy " +
                   "of the cluster.")

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

# Check parameter validity.
if params.nodeCount < 1:
    pc.reportError(portal.ParameterError("You must choose at least 1 node.", ["nodeCount"]))

if params.tempFileSystemSize < 0 or params.tempFileSystemSize > 200:
    pc.reportError(portal.ParameterError("Please specify a size greater then zero and " +
                                         "less then 200GB", ["tempFileSystemSize"]))

if params.phystype != "":
    tokens = params.phystype.split(",")
    if len(tokens) != 1:
        pc.reportError(portal.ParameterError("Only a single type is allowed", ["phystype"]))

pc.verifyParameters()

# Create link/lan.
if params.nodeCount > 1:
    if params.nodeCount == 2:
        lan = request.Link()
    else:
        lan = request.LAN()
        pass
    if params.bestEffort:
        lan.best_effort = True
    elif params.linkSpeed > 0:
        lan.bandwidth = params.linkSpeed
    if params.sameSwitch:
        lan.setNoInterSwitchLinks()
    pass

# Process nodes, adding to link or lan.
for i in range(params.nodeCount):
    # Create a node and add it to the request
    if params.useVMs:
        name = "vm" + str(i)
        node = request.XenVM(name)
        if params.exclusiveVMs:
            node.exclusive = True
            pass
    else:
        name = "node" + str(i)
        node = request.RawPC(name)
        pass
    if params.osImage and params.osImage != "default":
        node.disk_image = params.osImage
        pass
    # Add to lan
    if params.nodeCount > 1:
        iface = node.addInterface("eth1")
        lan.addInterface(iface)
        pass
    # Optional hardware type.
    if params.phystype != "":
        node.hardware_type = params.phystype
        pass
    # Optional Blockstore
    if params.tempFileSystemSize > 0 or params.tempFileSystemMax:
        bs = node.Blockstore(name + "-bs", params.tempFileSystemMount)
        if params.tempFileSystemMax:
            bs.size = "0GB"
        else:
            bs.size = str(params.tempFileSystemSize) + "GB"
            pass
        bs.placement = "any"
        pass
    #
    # Install and start X11 VNC. Calling this informs the Portal that you want a VNC
    # option in the node context menu to create a browser VNC client.
    #
    # If you prefer to start the VNC server yourself (on port 5901) then add nostart=True. 
    #
    if params.startVNC:
        node.startVNC()
        pass
    pass

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
