# slurm-maint-controller
controller to manage the reboot of nodes in a somewhat intelligent way

# Purpose

We often have to reboot nodes in a cluster, but we want to do it in a way that minimizes disruption to the users. This controller helps us achieve that by intelligently scheduling the reboots.

We make use of the fact that the node is useable upto the time we need to reboot the node. But we can't reboot the node if a job is already running on the node. We therefore determine the expected end time of the jobs on a node and set an advanced reservation just for that node. This will allow other jobs to also run on the node upto the time the reservation begins.

This hopefully will maximize the resources made available to users as we continually have to reboot nodes for patches, upgrades etc.

# To Do

Implement a counting system whereby one can determine what fraction of the cluster is allowed to be offline at any instance, and how many reservations we can maintain to limit the impact upon the reboot of nodes.

Also need a mechanisn to reboot the node reliably and to ensure that the system is completely up before taking another node offline.


# Installation

To use the controller, you need to install it on a node that has access to the Slurm database. Once installed, you can start the controller by running the following command:

```
make dev
```


# Usage

...
