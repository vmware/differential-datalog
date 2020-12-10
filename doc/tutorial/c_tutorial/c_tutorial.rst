********************************
WIP: How to befriend DDlog and C
********************************

.. contents::

Prerequisites
=============

First, you need to obtain DDlog__ from the official repository, which contains instructions how to install and set it up. There is also an expectation that you are familiar with DDlog syntax and commands, i.e., read the tutorial or at least skim through it.

__
.. _DDlog DDlog repository on GitHub:
    https://github.com/vmware/differential-datalog

Introduction
============

In this set of tutorials we are going to explore various features of DDlog and learn how to befriend DDlog program with C program. To do that we are going to build several simple yet functional applications as it is more fun to build real programs. Furthermore, it will also demonstrate that DDlog is not limited to a particular domain, but rather can be used to solve different problems.

Tutorial #1 - Reachibility Monitor
==================================

In our first tutorial we are going to build a program that will store the reachibility information in the network of four nodes: Menlo Park, Santa Barbara, Los Angeles, and Salt Lake City. User will update link status between nodes through CLI and our program will calculate the rest. The purpose of this program is to provide a simple interface to a network operator and return information regarding which node can reach which. Thus, the only concern of network operator is to update link status without having to worry about doing those calculations themself. While, this task can be relatively easy done in our example of four nodes, in a more realistic scenario, the number of nodes can be in the order of hundreds and link status updates can be streamed into the monitoring application.

Let's start with defining our program in the DDlog language. In our program we need to supply source node, destination node, and a link status between them. Now we are ready to define our input relation :code:`Links`. Let's create a file called :code:`t1_reachibility_monitor.dl` and add there a following line representing input relation for our reachibility monitor:

.. code-block::
    
	input relation Links(src: string, dst: string, link_status: bool)

However, for the reachibility monitor to be useful it also should provide some output and not just ingest the data. We want our program to output a list of nodes that are currently reachable based on the information in the relation :code:`Links`. Thus, let's add an output relation :code:`ConnectedNodes` and provide a rule to calculate records in this relation. To do that we are going to add few more lines to the :code:`t1_reachibility_monitor.dl` so it will looks as follows:

.. code-block::

	input relation Links(src: string, dst: string, link_status: bool)
	output relation ConnectedNodes(str: string, dst: string)

	ConnectedNodes(src, dst) :- Links(src, dst, true).
	ConnectedNodes(src, dst) :- ConnectedNodes(src, intermediate_node),
				Links(intermediate_node, dst, true).

Let's briefly discuss the rule to calculate :code:`ConnectedNodes` relation. First, we say that any directly connected nodes such that the :code:`link_status` between them is :code:`true` belong to :code:`ConnectedNodes` relation. Furthermore, any two nodes are considered connected if there is path from the source node to the destination node such that all directly connected nodes along the path are also connected. Take a minute here to realize how easy is it to write such rule in DDlog. Isn't is amazing?

With that done, let's compile and start out program.

.. code-block::

	$ ddlog -i t1_reachibility_monitor.dl
	$ cd t1_reachibility_monitor_ddlog/ && cargo build --release
	$ ./target/release/t1_reachibility_monitor_cli

If we check :code:`ConnectedNodes` we will see that this relation is empty, which make sense as we haven't populated relation :code:`Links` with anything yet:

.. code-block::

	>> dump ConnectedNodes;

To exit DDlog shell press `[CTRL+d]`. If you are using `Windows Terminal` do not press `[CTRL+D]` (note capital `D`, `[CTRL+SHIFT+d]`) as it will open new tab.

Let's fix it. We are going to insert several records into :code:`Links` relation. But first let's define a simple network topology that we are going to work through. To make our life more interesting (and just slightly more complicated) the links between nodes will not be bidirectional by default.

MSR: Embed the figure I made for the topology

A -> B <-> C
  \>     </>
       D


A: "Menlo Park"
B: "Santa Barbara"
C: "Los Angeles"
D: "Salt Lake City"

So our nodes will be the following:
A: {"192.168.0.10", "Los Angeles"}
B: {"192.168.0.20", "Menlo Park"}
C: {"192.168.0.30", "Santa Barbara"}
D: {"192.168.0.40", "Salt Lake City"}

Let's start again DDlog shell and insert records to recreate the network topology above:

.. code-block::

	$ ./target/release/t1_reachibility_monitor_cli
	>> start;
	>> insert Links("Menlo Park", "Santa Barbara", true);
	>> insert Links("Menlo Park", "Salt Lake City", true);
	>> insert Links("Santa Barbara", "Los Angeles", true);
	>> insert Links("Los Angeles", "Santa Barbara", true);
	>> insert Links("Los Angeles", "Salt Lake City", true);
	>> insert Links("Salt Lake City", "Los Angeles", true);
	>> commit;
	>> dump ConnectedNodes;
	ConnectedNodes{.src = "Los Angeles", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Los Angeles", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Los Angeles", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Salt Lake City", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Salt Lake City", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Salt Lake City", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Santa Barbara", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Santa Barbara", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Santa Barbara", .dst = "Santa Barbara"}

Something is off - some nodes are connected to themselves. But why this happened? If we take a closer look at our rule we can notice that this phenomenon actually make sense. For example, Santa Barbara is reachable from Santa Barbara through Los Angeles. While it is not necessarily horrible or wrong we may want to avoid it as it clutters the relation and the output. More notably, we definitely don't want the network traffic go to Santa Barbara from Santa Barbara through Los Angeles (in real world this actually may happen but this is completely different topic). Let's fix it by adding a filtering condition to the rule that disallows source and destination match each other. Now, the rules for calculating :code:`ConnectedNodes` look as below and see how simple it is to do that in DDlog (note that only second rule was modified, the first stay intact):

.. code-block::

	ConnectedNodes(src, dst) :- Links(src, dst, true).
	ConnectedNodes(src, dst) :- ConnectedNodes(src, intermediate_node),
				Links(intermediate_node, dst, true), (src != dst).

As we have changed the DDlog program we need to recompile it. However, it would be annoying to run commands that populate :code:`Links` relation every time we change our DDlog program. To resolve this nuisance we can embed some records into the DDlog program itself to serve as ground truth or initial state. Let's do all of that.

.. code-block::

	$ cd ../ && cat t1_reachibility_monitor.dl
	input relation Links(src: string, dst: string, link_status: bool)
	output relation ConnectedNodes(str: string, dst: string)

	ConnectedNodes(src, dst) :- Links(src, dst, true).
	ConnectedNodes(src, dst) :- ConnectedNodes(src, intermediate_node),
				Links(intermediate_node, dst, true), (src != dst).
	
	Links("Menlo Park", "Santa Barbara", true).
	Links("Menlo Park", "Salt Lake City", true).
	Links("Santa Barbara", "Los Angeles", true).
	Links("Los Angeles", "Santa Barbara", true).
	Links("Los Angeles", "Salt Lake City", true).
	Links("Salt Lake City", "Los Angeles", true).

	$ ddlog -i t1_reachibility_monitor.dl
	$ cd t1_reachibility_monitor_ddlog/ && cargo build --release
	$ ./target/release/t1_reachibility_monitor_cli
	<clip>
	>> dump ConnectedNodes;
	ConnectedNodes{.src = "Los Angeles", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Los Angeles", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Salt Lake City"}
	ConnectedNodes{.src = "Menlo Park", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Salt Lake City", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Salt Lake City", .dst = "Santa Barbara"}
	ConnectedNodes{.src = "Santa Barbara", .dst = "Los Angeles"}
	ConnectedNodes{.src = "Santa Barbara", .dst = "Salt Lake City"}

Perfect! Now it looks exactly as what we expected! This is a good time to start doing what this tutorial promised and start writing some C code! We are going to start with something simple yet important. Our initial C program will connect with DDlog program, insert one additional record to :code:`Links` relation, and print the content of :code:`ConnectedNodes` relation. Let's create :code:`reachibility_monitor.c` next to our DDlog program's code. The code is available in the provided :code:`reachibility_monitor_p1.c` file. The further discussion will refer to specific lines in that code.

Let's compile the code first and then delve into discussion of the code and compilation.

.. code-block::

	gcc reachibility_monitor.c t1_reachibility_monitor_ddlog/target/release/libt1_reachibility_monitor_ddlog.a -It1_reachibility_monitor_ddlog/ -lpthread -ldl -lm


This concludes the first part of this tutorial. In the next part we are going to modify our program to allow for not only execution of a single command but also being to update existing records using primary key, insert multiple records in a single transaction, and use the `delta` feature of DDlog for a convenient tracking of changes in the output relations.

* add the discussion of the code
* add the discussion of the compilation
* the code I believe has an issue with memory deallocation. That is something to add to the code and respective discussion
* add the rest of the tutorial