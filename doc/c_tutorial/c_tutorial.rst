************************
Integrating C with DDlog
************************

**Thank you, @smadaminov, for kindly contributing this tutorial!**

.. contents::

Prerequisites
=============

First, you need to obtain DDlog_ from the official repository, which contains instructions how to install and set it up.
There is also an expectation that you are familiar with DDlog syntax and commands, i.e., you have read the tutorial or at least skimmed through it.

.. _DDlog: https://github.com/vmware/differential-datalog

Introduction
============

In this set of tutorials we are going to explore various features of DDlog programming language and learn how to integrate a DDlog program into a C program.
To do that we are going to build a simple yet functional application as it is more fun to build real programs.
We will start by writing a simple program with a limited functionality to get familiar with the basics of DDlog.
Afterwards, over the course of this set of tutorials, we will gradually improve our application by applying more advanced techniques offered by DDlog.

Tutorial #1 - Reachability Monitor
==================================

In our first tutorial we are going to build a program that will calculate the reachability information in a network composed of nodes connected by links.
User will update link status between nodes through CLI and our program will calculate the rest.
The purpose of this program is to provide a simple interface to a network operator and return information regarding which node can reach which.
Thus, the only concern of network operator is to update link status without having to worry about doing those calculations themself.
We are going to test our program with the network consisting of four nodes: Menlo Park, Santa Barbara, Los Angeles, and Salt Lake City.
However, nothing prevents us from using the network comprised of hundreds of nodes.

Let's start with defining our program in the DDlog language.
Now we are ready to define our input relation :code:`Links`.
Let's create a file called :code:`t1_reachability_monitor.dl` and add there a following line representing input relation for our reachability monitor:

.. code-block::

    input relation Links(src: string, dst: string, link_status: bool)

However, for the reachability monitor to be useful it also should provide some output and not just ingest the data.
We want our program to output a list of nodes that are currently reachable based on the information in the relation :code:`Links`.
Thus, let's add an output relation :code:`ConnectedNodes` and provide rules how to calculate records in this relation *(mind the presence of the dot at the end of each rule)*.
To do that we are going to add few more lines to the :code:`t1_reachability_monitor.dl` so it will look as follows:

.. code-block::

    input relation Links(src: string, dst: string, link_status: bool)
    output relation ConnectedNodes(src: string, dst: string)

    ConnectedNodes(src, dst) :- Links(src, dst, true).
    ConnectedNodes(src, dst) :- ConnectedNodes(src, intermediate_node),
                                Links(intermediate_node, dst, true).

Let's briefly discuss the rule to calculate :code:`ConnectedNodes` relation.
First, we say that any two directly connected nodes such that the :code:`link_status` between them is :code:`true` belong to the :code:`ConnectedNodes` relation.
Furthermore, any two nodes are considered connected if there is path from the source node to the destination node such that all directly connected nodes along the path also have the :code:`link_status` between them to be :code:`true`.
Take a minute here to realize how easy it is to write such rule in DDlog.
While these rules may not require much effort in other programming languages, the distinct feature of DDlog is that the computing is done incrementally!

With that done, let's compile and start our program.
We will feed our DDlog code to the DDlog compiler and it will generate a new folder.
This folder contains CLI so we can run our program as well as a library that we will need later for our C program.
Note, that on the first run it will take a while, but will be signifanctly faster afterwards.
After the code is compiled let's start a command-line so we can start playing with our program.

.. code-block::

    $ ddlog -i t1_reachability_monitor.dl
    $ cd t1_reachability_monitor_ddlog/ && cargo build --release
    $ ./target/release/t1_reachability_monitor_cli

If we display the contents of the :code:`ConnectedNodes` relation we will see that this it is empty, which make sense as we haven't populated :code:`Links` relation with anything yet:

.. code-block::

    >> dump ConnectedNodes;

To exit DDlog shell press `[CTRL+d]`.
If you are using `Windows Terminal` do not press `[CTRL+D]` (note capital `D`, i.e., `[CTRL+SHIFT+d]`) as it will open a new tab.

Let's supply some input data.
We are going to insert several records into :code:`Links` relation.
But first let's define a simple network topology that we are going to work through.
To make our life more interesting (and just slightly more complicated) the links between nodes will not be bidirectional by default.

.. image:: ./images/topology.jpg

Note that DDlog exposes a `transaction-based`_ API.
Each transaction begins with a :code:`start` command and ends with a :code:`commit` command.
Let's start DDlog shell again and insert records to recreate the network topology above:

.. _transaction-based: https://en.wikipedia.org/wiki/Transaction_processing

.. code-block::

    $ ./target/release/t1_reachability_monitor_cli
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

In the output we can see all cities with direct links between them are connected.
Furthermore, as we specified in our DDlog code, if there is a path between two cities then they are also connected, e.g., Menlo Park is connected to Los Angeles.
However, some nodes are connected to themselves.
How did this happen?
If we take a closer look at our rules we can notice that this phenomenon actually makes sense.
For example, Santa Barbara is reachable from Santa Barbara through Los Angeles.
While it is not necessarily horrible or wrong we may want to avoid it as it clutters the relation and the output.
More notably, we definitely don't want the network traffic go to Santa Barbara from Santa Barbara through Los Angeles (in the real world this actually may happen but this is a completely different topic).
Let's fix it by adding a filtering condition to the rule that disallows source and destination match each other.
Now, the rules for calculating :code:`ConnectedNodes` look as below and see how simple it is to do that in DDlog (note that only second rule was modified, the first stays intact):

.. code-block::

    $ cd ../ && cat t1_reachability_monitor.dl
    // Input relations
    input relation Links(src: string, dst: string, link_status: bool)

    // Output relations
    output relation ConnectedNodes(src: string, dst: string)

    /*
     * Rules to calculate `ConnectedNodes` relation
     */
    ConnectedNodes(src, dst) :- Links(src, dst, true).
    ConnectedNodes(src, dst) :- ConnectedNodes(src, intermediate_node),
                                Links(intermediate_node, dst, true), (src != dst).

.. tip:: This is a tip on DDlog syntax concerning comments in the code.

    DDlog supports C-style comments as you can see in the example above.

As we have changed the DDlog program we need to recompile it.

.. code-block::

    $ ddlog -i t1_reachability_monitor.dl
    $ cd t1_reachability_monitor_ddlog/ && cargo build --release
    $ ./target/release/t1_reachability_monitor_cli
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

Perfect!
Now it looks exactly as we expected!
Rigth before we jump to what this tutorial promised let's look at one more example that demonstrates incremental nature of DDlog.

.. code-block::

    $ ./target/release/t1_reachability_monitor_cli
    >> start;
    >> insert Links("Menlo Park", "Santa Barbara", true);
    >> insert Links("Menlo Park", "Salt Lake City", true);
    >> insert Links("Santa Barbara", "Los Angeles", true);
    >> insert Links("Los Angeles", "Santa Barbara", true);
    >> insert Links("Los Angeles", "Salt Lake City", true);
    >> insert Links("Salt Lake City", "Los Angeles", true);
    >> commit;
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
    >> start;
    >> delete Links("Santa Barbara", "Los Angeles", true);
    >> commit dump_changes;
    ConnectedNodes:
    ConnectedNodes{.src = "Santa Barbara", .dst = "Los Angeles"}: -1
    ConnectedNodes{.src = "Santa Barbara", .dst = "Salt Lake City"}: -1

DDlog incrementally computed only changes that happened and printed them.
The "minus one" here is called :code:`weight` and indicates that the respective record was deleted.
While the benefits of incremental computation ain't noticeable in our small example, they manifest themselves on a large scale and can make a substantial difference.

Now we are finally realdy to start writing some C code!
We are going to start with something simple yet important.
Our initial C program will connect to DDlog program, insert one additional record to :code:`Links` relation, and print the content of :code:`ConnectedNodes` relation.
Let's create :code:`t1_reachability_monitor.c` file next to our DDlog program's code.
The full source code is available in the provided :code:`t1_reachability_monitor.c` file.
The further discussion will refer to specific lines in that code.

Let's compile the code first and then delve into the discussion of compilation and the code.

.. code-block::

    $ cd ../
    $ gcc t1_reachability_monitor.c t1_reachability_monitor_ddlog/target/release/libt1_reachability_monitor_ddlog.a -It1_reachability_monitor_ddlog/ -lpthread -ldl -lm


.. tip:: This is a tip on a compilation failure caused by a missing package.

    If the compilation fails, you may want to make sure that you have :code:`libc6-dev` package installed.
    This is the package name for Ubuntu 18.04.
    For other releases and operating systems you may need to refer the respective documentation (or Google Search).

When we compiled our DDlog program, DDlog compiler automatically generated a static library that contains DDlog API for C.
Thus, we need to link it with our program.
This API is defined in the :code:`ddlog.h` header file generated by the DDlog compiler and we provide the path to it using the :code:`-I` flag.
Furthermore, :code:`ddlog.h` is heavily documented and is worth going through as it explains API in great details.

Let's run the compiled code, provide an input, and see what happens:

.. code-block::

    $ ./a.out
    Links table ID: 1
    ConnectedNodes table ID: 0
    Please enter source name > Menlo Park
    Please enter destination name > Santa Barbara
    Please enter the link status between source and destination > true
    Inserting the following record: Links{"Menlo Park", "Santa Barbara", true}
    Content of the ConnectedNodes relation:
    Inserted record: ConnectedNodes{.src = "Menlo Park", .dst = "Santa Barbara"}

We just executed our first DDlog-C application!
It asked us for some input (that we, of course, provided) and then produced an output.
More specifically, this application printed the content of the :code:`ConnectedNodes` relation.

With that, let's take a closer look on the code in the provided file and go over it.
To make this process easier, we put comments in the code, which should also help to navigate the code (just search for the respective text).
We will go through the code in small snippets and will skip some minor parts, which are purely related to the C code and are self-explanatory.

.. code-block:: c

    // Start the DDlog program and connect to it
    ddlog_prog prog = ddlog_run(1, true, NULL, NULL);
    if (prog == NULL) {
        fprintf(stderr, "failed to initialize DDlog program\n");
        exit(EXIT_FAILURE);
    };

We begin with starting the DDlog program and connecting to it using :code:`ddlog_run()` function.
Note that it returns a pointer that stored in :code:`prog` variable, which we will use later in the code.
We need to supply four arguments to this function:

#. Number of worker threads for the DDlog program. In our case, one worker thread is more than sufficient.
#. Flag to specify that we want to store the copy of output tables in the DDlog so we can use :code:`ddlog_dump_table()` function. If you have an application of streaming nature then you may want to set this flag to :code:`false` to avoid imposed memory and CPU overheads.
#. A pointer to store the initial state of the program, i.e., content of the output relations. As we start from a clean slate we set this pointer to :code:`NULL`.
#. A callback to use for redirecting diagnostic messages from DDlog to it. We currently don't need that so we set this one to :code:`NULL`.

.. code-block:: c

    // Get table IDs for `Links` and `ConnectedNodes` relations
    table_id LinksTableID = ddlog_get_table_id(ddlog, "Links");
    table_id ConnectedNodesTableID = ddlog_get_table_id(ddlog, "ConnectedNodes");

DDlog stores relations in tables.
Thus, to work with those tables we will need their IDs.

.. code-block:: c

    // Prompt user to enter record values
    // and collect them from the standard input
    printf("Please enter source name > ");
    if (getline(&src_line_ptr, &n_src, stdin) < 0) {
        if (src_line_ptr != NULL) free(src_line_ptr);
        return -EINVAL;
    }
    printf("Please enter destination name > ");
    if (getline(&dst_line_ptr, &n_dst, stdin) < 0) {
        free(src_line_ptr);
        if (dst_line_ptr != NULL) free(dst_line_ptr);
        return -EINVAL;
    }
    printf("Please enter the link status between source and destination > ");
    if (getline(&link_status_line_ptr, &n_link_status, stdin) < 0) {
        free(src_line_ptr);
        free(dst_line_ptr);
        if (link_status_line_ptr != NULL) free(link_status_line_ptr);
        return -EINVAL;
    }

In this tutorial, application will prompt user to enter values for the record and collect them.

.. code-block:: c

    // Parsing value for the link status
    // Anything different from `true` will be considered as `false`
    if (strlen(link_status_line_ptr) == 5) {
        link_status = (strncmp("true", link_status_line_ptr, 4) == 0) ? true : false;
    }

For the sake of simplicity, we will view any input entered by the user that differs from :code:`true` as :code:`false`.

.. code-block:: c

    // Creating record values in the DDlog format
    ddlog_record *src = ddlog_string(src_line_ptr);
    ddlog_record *dst = ddlog_string(dst_line_ptr);
    ddlog_record *lstatus = ddlog_bool(link_status);

One of the features of the DDlog is being a `strongly-typed language`_.
Partially, due to that reason we cannot just pass anything to our DDlog program.
So we have to convert C objects to the DDlog objects first.

.. _strongly-typed language: https://en.wikipedia.org/wiki/Strong_and_weak_typing

.. code-block:: c

    // Constructing a single record from separate values
    ddlog_record **struct_args;
    struct_args = (ddlog_record**)malloc(3 * sizeof(ddlog_record*));
    struct_args[0] = src;
    struct_args[1] = dst;
    struct_args[2] = lstatus;
    ddlog_record *new_record = ddlog_struct("Links", struct_args, 3);

Once, we have DDlog values in place we need to construct a record that we will insert into the :code:`Links` relation.
For that purpose we can use :code:`ddlog_struct()` function.
It takes three arguments:

#. Name of the relation, which this record belongs to.
#. A pointer to an array of :code:`ddlog_record`'s.
#. Length of the aformentioned array.

You don't have to dynamically allocate memory for the :code:`struct_args` in this example.
As we know exactly how many elements this array contains we can simply put it on the stack.

.. code-block:: c

    // Let's print the record that we are about to insert
    // to the `Links` relation
    char *record_to_insert_as_string = ddlog_dump_record(new_record);
    printf("Inserting the following record: %s\n", record_to_insert_as_string);
    ddlog_string_free(record_to_insert_as_string);

Using the :code:`ddlog_dump_record()` function from DDlog API we can retrieve the record as a string and print it to make sure everything looks right.
Note, that this function will allocate some memory on a heap and we are responsible for freeing it.

.. code-block:: c

    // Start transaction
    if (ddlog_transaction_start(prog) < 0) {
        fprintf(stderr, "failed to start transaction\n");
        exit(EXIT_FAILURE);
    };

As mentioned before, DDlog is a transaction-based programming language.
Thus, before inserting a new record into a relation we need to start transaction.

.. code-block:: c

    // Create `insert` command
    ddlog_cmd *cmd = ddlog_insert_cmd(LinksTableID, new_record);
    if (cmd == NULL) {
        fprintf(stderr, "failed to create insert command\n");
        exit(EXIT_FAILURE);
    }

Next step is to create an insertion DDlog command using :code:`ddlog_insert_cmd()` function.
We need to provide this function with the table ID of a target relation and a record that we want to insert into that relation.

.. code-block:: c

    // Apply updates to the relation with records
    // specified in the provided command `cmd`
    if (ddlog_apply_updates(prog, &cmd, 1) < 0) {
        fprintf(stderr, "failed to apply updates\n");
        exit(EXIT_FAILURE);
    };

Once we have command ready we can apply it to our DDlog program using :code:`ddlog_apply_updates()` function.
We need to supply program handle, array of commands to be applied, and the length of this array.
In our case, we only have a single command to apply.
However, it is possible (and is more efficient) to pass multiple commands to a single call to the :code:`ddlog_apply_update()` function.
Note, that in the case of multiple commands if some of them fail then some subset of commands may still be applied.
Please refer to the API description in the :code:`ddlog.h` header file for more details.

.. code-block:: c

    // Commit transaction
    if (ddlog_transaction_commit(prog) < 0) {
        fprintf(stderr, "failed to commit transaction\n");
        exit(EXIT_FAILURE);
    };

As we have applied all the commands that we wanted (again, just one command in our case) we are ready to commit the transaction.
This will persist changes in the relations.

.. code-block:: c

    // Print records in the `ConnectedNodes` relation
    printf("Content of the ConnectedNodes relation:\n");
    ddlog_dump_table(prog, ConnectedNodesTableID, &print_records_callback, (uintptr_t)(void*)(NULL));

This part of the code allows us to see the content of the :code:`ConnectedNodes` relation using :code:`ddlog_dump_table()` function.
We need to provide the following four arguments:

#. DDlog program handle.
#. Table ID of the target relation.
#. A callback function that will be invoked for records returned by the :code:`ddlog_dump_table()`. We are going to discuss this function in more details further in the text.
#. A pointer, which will be passed as an argument to each invokation of the callback function. In our case, we don't need any so we set it to :code:`NULL`.

.. code-block:: c

	// Callback function that will be invoked for every record returned
	// by the call to `ddlog_dump_table()` function
	bool print_records_callback(uintptr_t arg, const ddlog_record *rec, ssize_t weight) {
	    char *record_as_string = ddlog_dump_record(rec);
	    if (record_as_string == NULL) {
	        fprintf(stderr, "failed to dump record\n");
	        exit(EXIT_FAILURE);
	    }
	    char *action = (weight == 1) ? "Inserted" : "Deleted";
	    printf("%s record: %s\n", action, record_as_string);
	    ddlog_string_free(record_as_string);
	
	    return true;
	}

This is the callback function that we supplied to the :code:`ddlog_dump_table()` function.
It will be invoked for every record.
The body of the callback function can be anything as long as it returns either :code:`true` or :code:`false`.
Whenever the callback function returns :code:`true` it asks DDlog to continue enumeration of the records.
In other words, it will be invoked again if there are more records available.
However, if for some some reason you want to stop enumeration or an invokation of the callback function, then you can implement a condition in the function such that it will return :code:`false`.

You can notice that there is one argument in the callback function that we have mentioned previously.
The value of the :code:`weight` arguments indicates whether the record was inserted or deleted.

.. code-block:: c

    // Freeing memory
    free(struct_args);
    free(src_line_ptr);
    free(dst_line_ptr);
    free(link_status_line_ptr);

Now we are getting closer to the end of our program.
Here we follow a good practice of cleaning up after ourselves and freeing the memory.
Note, that we haven't done same for the most DDlog objects, namely, records and insert command.
This is because, they were consumed by the respective DDlog commands that we used.
That is, memory deallocation happened behind the scenes. Convenient, isn't it?

.. code-block:: c

    // Stop the DDlog program
    if (ddlog_stop(prog) < 0) {
        fprintf(stderr, "failed to stop DDlog program\n");
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;

At this point, we are ready to exit the program as we did everything we wanted.
But just before doing so, we stop the DDlog program.

As we mentioned before, :code:`ddlog.h` explains all these API in a more detailed fashion.

This concludes the first part of this set of tutorials.
We just run our first DDlog-C program that can already do something meaningful.
More importantly, we saw how to use DDlog and C together and run an application that does that very thing.

