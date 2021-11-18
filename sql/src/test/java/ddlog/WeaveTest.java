/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ddlog;

import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.PrestoSqlStatement;
import org.junit.Assert;
import org.junit.Test;

public class WeaveTest extends BaseQueriesTest {
    private final Translator t = new Translator();

    private DDlogIRNode translatePrestoSqlStatement(String sql) {
        return t.translateSqlStatement(new PrestoSqlStatement(sql));
    }

    @Test
    public void testWeave() {
        String node_info = "create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null with (primary_key = true),\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")";
        DDlogIRNode create = translatePrestoSqlStatement(node_info);
        Assert.assertNotNull(create);

        String pod_info =
                "create table pod_info\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null with (primary_key = true),\n" +
                        "  status varchar(36) not null,\n" +
                        "  node_name varchar(36) /*null*/,\n" +
                        "  namespace varchar(100) not null,\n" +
                        "  cpu_request bigint not null,\n" +
                        "  memory_request bigint not null,\n" +
                        "  ephemeral_storage_request bigint not null,\n" +
                        "  pods_request bigint not null,\n" +
                        "  owner_name varchar(100) not null,\n" +
                        "  creation_timestamp varchar(100) not null,\n" +
                        "  priority integer not null,\n" +
                        "  schedulerName varchar(50),\n" +
                        "  has_node_selector_labels boolean not null,\n" +
                        "  has_pod_affinity_requirements boolean not null\n" +
                        ")";
        create = translatePrestoSqlStatement(pod_info);
        Assert.assertNotNull(create);

        String  pod_ports_request =
                "-- This table tracks the \"ContainerPorts\" fields of each pod.\n" +
                        "-- It is used to enforce the PodFitsHostPorts constraint.\n" +
                        "create table pod_ports_request\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  host_ip varchar(100) not null,\n" +
                        "  host_port integer not null,\n" +
                        "  host_protocol varchar(10) not null\n" +
                        "  -- ,foreign key(pod_name) references pod_info(pod_name) on delete cascade\n" +
                        ")";
        create = translatePrestoSqlStatement(pod_ports_request);
        Assert.assertNotNull(create);

        String container_host_ports =
                "-- This table tracks the set of hostports in use at each node.\n" +
                        "-- Also used to enforce the PodFitsHostPorts constraint.\n" +
                        "create table container_host_ports\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  host_ip varchar(100) not null,\n" +
                        "  host_port integer not null,\n" +
                        "  host_protocol varchar(10) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(container_host_ports);
        Assert.assertNotNull(create);

        String pod_node_selector_labels =
                "-- Tracks the set of node selector labels per pod.\n" +
                        "create table pod_node_selector_labels\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  term integer not null,\n" +
                        "  match_expression integer not null,\n" +
                        "  num_match_expressions integer not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(12) not null,\n" +
                        "  label_value varchar(36) /*null*/\n" +
                        "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                        ")";
        create = translatePrestoSqlStatement(pod_node_selector_labels);
        Assert.assertNotNull(create);

        String pod_affinity_match_expressions =
                "-- Tracks the set of pod affinity match expressions.\n" +
                        "create table pod_affinity_match_expressions\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_selector integer not null,\n" +
                        "  match_expression integer not null,\n" +
                        "  num_match_expressions integer not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(30) not null,\n" +
                        "  label_value varchar(36) not null,\n" +
                        "  topology_key varchar(100) not null\n" +
                        "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                        ")";
        create = translatePrestoSqlStatement(pod_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_anti_affinity_match_expressions =
                "-- Tracks the set of pod anti-affinity match expressions.\n" +
                        "create table pod_anti_affinity_match_expressions\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_operator varchar(12) not null,\n" +
                        "  label_value varchar(36) not null,\n" +
                        "  topology_key varchar(100) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(pod_anti_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_labels =
                "-- Tracks the set of labels per pod, and indicates if\n" +
                        "-- any of them are also node selector labels\n" +
                        "create table pod_labels\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(pod_labels);
        Assert.assertNotNull(create);

        String node_labels =
                "-- Tracks the set of labels per node\n" +
                        "create table node_labels\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(node_labels);
        Assert.assertNotNull(create);

        String volume_labels =
                "-- Volume labels\n" +
                        "create table volume_labels\n" +
                        "(\n" +
                        "  volume_name varchar(36) not null,\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  label_value varchar(36) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(volume_labels);
        Assert.assertNotNull(create);

        String pod_by_service =
                "-- For pods that have ports exposed\n" +
                        "create table pod_by_service\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  service_name varchar(100) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(pod_by_service);
        Assert.assertNotNull(create);

        String service_affinity_labels =
                "-- Service affinity labels\n" +
                        "create table service_affinity_labels\n" +
                        "(\n" +
                        "  label_key varchar(100) not null\n" +
                        ")";
        create = translatePrestoSqlStatement(service_affinity_labels);
        Assert.assertNotNull(create);

        String labels_to_check_for_presence =
                "-- Labels present on node\n" +
                        "create table labels_to_check_for_presence\n" +
                        "(\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  present boolean not null\n" +
                        ")";
        create = translatePrestoSqlStatement(labels_to_check_for_presence);
        Assert.assertNotNull(create);

        String node_taints =
                "-- Node taints\n" +
                        "create table node_taints\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  taint_key varchar(100) not null,\n" +
                        "  taint_value varchar(100),\n" +
                        "  taint_effect varchar(100) not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(node_taints);
        Assert.assertNotNull(create);

        String pod_taints =
                "-- Pod taints.\n" +
                        "create table pod_tolerations\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  tolerations_key varchar(100),\n" +
                        "  tolerations_value varchar(100),\n" +
                        "  tolerations_effect varchar(100),\n" +
                        "  tolerations_operator varchar(100)/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(pod_taints);
        Assert.assertNotNull(create);

        String node_images =
                "-- Tracks the set of node images that are already\n" +
                        "-- available at a node\n" +
                        "create table node_images\n" +
                        "(\n" +
                        "  node_name varchar(36) not null,\n" +
                        "  image_name varchar(200) not null,\n" +
                        "  image_size bigint not null/*,\n" +
                        "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(node_images);
        Assert.assertNotNull(create);

        String pod_images =
                "-- Tracks the container images required by each pod\n" +
                        "create table pod_images\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  image_name varchar(200) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = translatePrestoSqlStatement(pod_images);
        Assert.assertNotNull(create);

        String pods_to_assign_no_limit =
                "-- Select all pods that need to be scheduled.\n" +
                        "-- We also indicate boolean values to check whether\n" +
                        "-- a pod has node selector or pod affinity labels,\n" +
                        "-- and whether pod affinity rules already yields some subset of\n" +
                        "-- nodes that we can assign pods to.\n" +
                        "create view pods_to_assign_no_limit as\n" +
                        "select DISTINCT\n" +
                        "  pod_name,\n" +
                        "  status,\n" +
                        "  node_name as controllable__node_name,\n" +
                        "  namespace,\n" +
                        "  cpu_request,\n" +
                        "  memory_request,\n" +
                        "  ephemeral_storage_request,\n" +
                        "  pods_request,\n" +
                        "  owner_name,\n" +
                        "  creation_timestamp,\n" +
                        "  has_node_selector_labels,\n" +
                        "  has_pod_affinity_requirements\n" +
                        "from pod_info\n" +
                        "where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler'\n" +
                        "-- order by creation_timestamp\n";
        create = translatePrestoSqlStatement(pods_to_assign_no_limit);
        Assert.assertNotNull(create);

        String batch_size =
                "\n" +
                        "-- This view is updated dynamically to change the limit. This\n" +
                        "-- pattern is required because there is no clean way to enforce\n" +
                        "-- a dynamic \"LIMIT\" clause.\n" +
                        "create table batch_size\n" +
                        "(\n" +
                        "  pendingPodsLimit integer not null with (primary_key = true)\n" +
                        ")";
        create = translatePrestoSqlStatement(batch_size);
        Assert.assertNotNull(create);

        String pods_to_assign =
                "create view pods_to_assign as\n" +
                        "select DISTINCT * from pods_to_assign_no_limit -- limit 100\n";
        create = translatePrestoSqlStatement(pods_to_assign);
        Assert.assertNotNull(create);

        String pods_with_port_request =
                "-- Pods with port requests\n" +
                        "create view pods_with_port_requests as\n" +
                        "select DISTINCT pods_to_assign.controllable__node_name as controllable__node_name,\n" +
                        "       pod_ports_request.host_port as host_port,\n" +
                        "       pod_ports_request.host_ip as host_ip,\n" +
                        "       pod_ports_request.host_protocol as host_protocol\n" +
                        "from pods_to_assign\n" +
                        "join pod_ports_request\n" +
                        "     on pod_ports_request.pod_name = pods_to_assign.pod_name";
        create = translatePrestoSqlStatement(pods_with_port_request);
        Assert.assertNotNull(create);

        String pod_node_selectors =
                "-- Pod node selectors\n" +
                "create view pod_node_selector_matches as\n" +
                "select DISTINCT pods_to_assign.pod_name as pod_name,\n" +
                "       node_labels.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_node_selector_labels\n" +
                "     on pods_to_assign.pod_name = pod_node_selector_labels.pod_name\n" +
                "join node_labels\n" +
                "        on\n" +
                "           (pod_node_selector_labels.label_operator = 'In'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "            and pod_node_selector_labels.label_value = node_labels.label_value)\n" +
                "        or (pod_node_selector_labels.label_operator = 'Exists'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key)\n" +
                "        or (pod_node_selector_labels.label_operator = 'NotIn')\n" +
                "        or (pod_node_selector_labels.label_operator = 'DoesNotExist')\n" +
                "group by pods_to_assign.pod_name,  node_labels.node_name, pod_node_selector_labels.term,\n" +
                "         pod_node_selector_labels.label_operator, pod_node_selector_labels.num_match_expressions\n" +
                "having case pod_node_selector_labels.label_operator\n" +
                "            when 'NotIn'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "                              and pod_node_selector_labels.label_value = node_labels.label_value))\n" +
                "            when 'DoesNotExist'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key))\n" +
                "            else count(distinct match_expression) = pod_node_selector_labels.num_match_expressions\n" +
                "       end";
        create = translatePrestoSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

        // indexes are not supported by Presto
        //String indexes =
        //        "create index pod_info_idx on pod_info (status, node_name);\n" +
        //        "create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_name);\n" +
        //        "create index node_labels_idx on node_labels (label_key, label_value);\n" +
        //        "create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);\n" +
        //        "create index pod_labels_idx on pod_labels (label_key, label_value);\n";
        //create = t.translateSqlStatement(indexes);
        //Assert.assertNotNull(create);

        String inter_pod_affinity =
                "-- Inter pod affinity\n" +
                "create view inter_pod_affinity_matches_inner as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       pod_labels.pod_name as matches,\n" +
                "       pod_info.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_affinity_match_expressions\n" +
                "     on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name\n" +
                "join pod_labels\n" +
                "        on (pod_affinity_match_expressions.label_operator = 'In'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "            and pod_affinity_match_expressions.label_value = pod_labels.label_value)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'Exists'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'NotIn')\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'DoesNotExist')\n" +
                "join pod_info\n" +
                "        on pod_labels.pod_name = pod_info.pod_name\n" +
                "group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_affinity_match_expressions.label_selector,\n" +
                "         pod_affinity_match_expressions.topology_key, pod_affinity_match_expressions.label_operator,\n" +
                "         pod_affinity_match_expressions.num_match_expressions, pod_info.node_name\n" +
                "having case pod_affinity_match_expressions.label_operator\n" +
                "             when 'NotIn'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "                               and pod_affinity_match_expressions.label_value = pod_labels.label_value))\n" +
                "             when 'DoesNotExist'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key))\n" +
                "             else count(distinct match_expression) = pod_affinity_match_expressions.num_match_expressions\n" +
                "       end";
        create = translatePrestoSqlStatement(inter_pod_affinity);
        Assert.assertNotNull(create);

        String inter_pod_affinity_matches =
                "create view inter_pod_affinity_matches as\n" +
                "select distinct *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner";
        create = translatePrestoSqlStatement(inter_pod_affinity_matches);
        Assert.assertNotNull(create);
        String spare_capacity =
                "-- Spare capacity\n" +
                "create view spare_capacity_per_node as\n" +
                "select DISTINCT node_info.name as name,\n" +
                "       cast(node_info.cpu_allocatable - sum(pod_info.cpu_request) as integer) as cpu_remaining,\n" +
                "       cast(node_info.memory_allocatable - sum(pod_info.memory_request) as integer) as memory_remaining,\n" +
                "       cast(node_info.pods_allocatable - sum(pod_info.pods_request) as integer) as pods_remaining\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'\n" +
                "group by node_info.name, node_info.cpu_allocatable,\n" +
                "         node_info.memory_allocatable, node_info.pods_allocatable";
        create = translatePrestoSqlStatement(spare_capacity);
        Assert.assertNotNull(create);

        String nodes_that_have_tollerations =
                "create view nodes_that_have_tolerations as\n" +
                        "select distinct node_name from node_taints";
        create = translatePrestoSqlStatement(nodes_that_have_tollerations);
        Assert.assertNotNull(create);

        String view_pods_that_tolerate_node_taints =
                "-- Taints and tolerations\n" +
                        "create view pods_that_tolerate_node_taints as\n" +
                        "select distinct pod_tolerations.pod_name as pod_name,\n" +
                        "       A.node_name as node_name\n" +
                        "from pods_to_assign\n" +
                        "join pod_tolerations\n" +
                        "     on pods_to_assign.pod_name = pod_tolerations.pod_name\n" +
                        "join (select distinct *, count(*) over (partition by node_name) as num_taints from node_taints) as A\n" +
                        "     on pod_tolerations.tolerations_key = A.taint_key\n" +
                        "     and (pod_tolerations.tolerations_effect = null\n" +
                        "          or pod_tolerations.tolerations_effect = A.taint_effect)\n" +
                        "     and (pod_tolerations.tolerations_operator = 'Exists'\n" +
                        "          or pod_tolerations.tolerations_value = A.taint_value)\n" +
                        "group by pod_tolerations.pod_name, A.node_name, A.num_taints\n" +
                        "having count(*) = A.num_taints";
        create = translatePrestoSqlStatement(view_pods_that_tolerate_node_taints);
        Assert.assertNotNull(create);

        String assigned_pods = "create view assigned_pods as\n" +
                "select distinct\n" +
                "  pod_name,\n" +
                "  status,\n" +
                "  node_name,\n" +
                "  namespace,\n" +
                "  cpu_request,\n" +
                "  memory_request,\n" +
                "  ephemeral_storage_request,\n" +
                "  pods_request,\n" +
                "  owner_name,\n" +
                "  creation_timestamp,\n" +
                "  has_node_selector_labels,\n" +
                "  has_pod_affinity_requirements\n" +
                "from pod_info\n" +
                "where node_name is not null\n";
        create = translatePrestoSqlStatement(assigned_pods);
        Assert.assertNotNull(create);

        DDlogProgram program = t.getDDlogProgram();
        String p = program.toString();
        Assert.assertNotNull(p);
        String expected = "import fp\n" +
                "import time\n" +
                "import sql\n" +
                "import sqlop\n" +
                "\n" +
                "typedef TRnode_info = TRnode_info{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, memory_capacity:bigint, ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint}\n" +
                "typedef TRpod_info = TRpod_info{pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "typedef TRpod_ports_request = TRpod_ports_request{pod_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef TRcontainer_host_ports = TRcontainer_host_ports{pod_name:string, node_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef TRpod_node_selector_labels = TRpod_node_selector_labels{pod_name:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:Option<string>}\n" +
                "typedef TRpod_affinity_match_expressions = TRpod_affinity_match_expressions{pod_name:string, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
                "typedef TRpod_anti_affinity_match_expressions = TRpod_anti_affinity_match_expressions{pod_name:string, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
                "typedef TRpod_labels = TRpod_labels{pod_name:string, label_key:string, label_value:string}\n" +
                "typedef TRnode_labels = TRnode_labels{node_name:string, label_key:string, label_value:string}\n" +
                "typedef TRvolume_labels = TRvolume_labels{volume_name:string, pod_name:string, label_key:string, label_value:string}\n" +
                "typedef TRpod_by_service = TRpod_by_service{pod_name:string, service_name:string}\n" +
                "typedef TRservice_affinity_labels = TRservice_affinity_labels{label_key:string}\n" +
                "typedef TRlabels_to_check_for_presence = TRlabels_to_check_for_presence{label_key:string, present:bool}\n" +
                "typedef TRnode_taints = TRnode_taints{node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string}\n" +
                "typedef TRpod_tolerations = TRpod_tolerations{pod_name:string, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>}\n" +
                "typedef TRnode_images = TRnode_images{node_name:string, image_name:string, image_size:bigint}\n" +
                "typedef TRpod_images = TRpod_images{pod_name:string, image_name:string}\n" +
                "typedef TRtmp = TRtmp{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "typedef TRbatch_size = TRbatch_size{pendingPodsLimit:signed<64>}\n" +
                "typedef Ttmp = Ttmp{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef TRtmp1 = TRtmp1{controllable__node_name:Option<string>, host_port:signed<64>, host_ip:string, host_protocol:string}\n" +
                "typedef Ttmp0 = Ttmp0{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:Option<string>}\n" +
                "typedef Ttmp1 = Ttmp1{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:Option<string>, node_name:string, label_key0:string, label_value0:string}\n" +
                "typedef TRtmp3 = TRtmp3{pod_name:string, node_name:string}\n" +
                "typedef Tagg = Tagg{col:Option<bool>}\n" +
                "typedef Ttmp2 = Ttmp2{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
                "typedef Ttmp3 = Ttmp3{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string, pod_name0:string, label_key0:string, label_value0:string}\n" +
                "typedef Ttmp4 = Ttmp4{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string, pod_name0:string, label_key0:string, label_value0:string, status0:string, node_name:Option<string>, namespace0:string, cpu_request0:bigint, memory_request0:bigint, ephemeral_storage_request0:bigint, pods_request0:bigint, owner_name0:string, creation_timestamp0:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels0:bool, has_pod_affinity_requirements0:bool}\n" +
                "typedef TRtmp6 = TRtmp6{pod_name:string, matches:string, node_name:Option<string>}\n" +
                "typedef Tagg0 = Tagg0{col:bool}\n" +
                "typedef TRtmp7 = TRtmp7{gb:string, pod_name:string, matches:string, node_name:Option<string>}\n" +
                "typedef TRtmp8 = TRtmp8{gb:string, count:signed<64>}\n" +
                "typedef Tagg1 = Tagg1{count:signed<64>}\n" +
                "typedef Ttmp5 = Ttmp5{gb:string, pod_name:string, matches:string, node_name:Option<string>, count:signed<64>}\n" +
                "typedef TRtmp9 = TRtmp9{pod_name:string, matches:string, node_name:Option<string>, num_matches:signed<64>}\n" +
                "typedef Ttmp6 = Ttmp6{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, memory_capacity:bigint, ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint, pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "typedef TRtmp10 = TRtmp10{name:string, cpu_remaining:signed<64>, memory_remaining:signed<64>, pods_remaining:signed<64>}\n" +
                "typedef Tagg2 = Tagg2{cpu_remaining:signed<64>, memory_remaining:signed<64>, pods_remaining:signed<64>}\n" +
                "typedef TRtmp11 = TRtmp11{node_name:string}\n" +
                "typedef Ttmp7 = Ttmp7{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>}\n" +
                "typedef TRtmp12 = TRtmp12{gb:string, node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string}\n" +
                "typedef Ttmp8 = Ttmp8{gb:string, node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string, count:signed<64>}\n" +
                "typedef TRtmp14 = TRtmp14{node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string, num_taints:signed<64>}\n" +
                "typedef Ttmp9 = Ttmp9{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>, node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string, num_taints:signed<64>}\n" +
                "typedef TRtmp18 = TRtmp18{pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "function agg(g: Group<(string, string, signed<64>, string, signed<64>), (Ttmp0, Ttmp0, TRnode_labels, Ttmp1)>):Tagg {\n" +
                "(var gb, var gb0, var gb1, var gb2, var gb3) = group_key(g);\n" +
                "(var any = Some{false}: Option<bool>);\n" +
                "(var any0 = false: bool);\n" +
                "(var count_distinct = set_empty(): Set<signed<64>>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v1 = i.0;\n" +
                "(var v3 = i.1);\n" +
                "(var v2 = i.2);\n" +
                "(var v4 = i.3);\n" +
                "(var incr = b_and_RN((v4.label_key == v4.label_key0), s_eq_NR(v4.label_value, v4.label_value0)));\n" +
                "(any = agg_any_N(any, incr));\n" +
                "(var incr0 = (v4.label_key == v4.label_key0));\n" +
                "(any0 = agg_any_R(any0, incr0));\n" +
                "(var incr1 = v4.match_expression);\n" +
                "(set_insert(count_distinct, incr1))}\n" +
                ");\n" +
                "(Tagg{.col = if ((gb2 == \"NotIn\")) {\n" +
                "b_not_N(any)} else {\n" +
                "if ((gb2 == \"DoesNotExist\")) {\n" +
                "Some{.x = (not any0)}} else {\n" +
                "Some{.x = (set_size(count_distinct) as signed<64> == gb3)}}}})\n" +
                "}\n" +
                "\n" +
                "function agg0(g: Group<(string, string, signed<64>, string, string, signed<64>, Option<string>), Ttmp4>):Tagg0 {\n" +
                "(var gb, var gb0, var gb1, var gb2, var gb3, var gb4, var gb5) = group_key(g);\n" +
                "(var any = false: bool);\n" +
                "(var any0 = false: bool);\n" +
                "(var count_distinct = set_empty(): Set<signed<64>>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v7 = i;\n" +
                "(var incr = ((v7.label_key == v7.label_key0) and (v7.label_value == v7.label_value0)));\n" +
                "(any = agg_any_R(any, incr));\n" +
                "(var incr0 = (v7.label_key == v7.label_key0));\n" +
                "(any0 = agg_any_R(any0, incr0));\n" +
                "(var incr1 = v7.match_expression);\n" +
                "(set_insert(count_distinct, incr1))}\n" +
                ");\n" +
                "(Tagg0{.col = if ((gb3 == \"NotIn\")) {\n" +
                "(not any)} else {\n" +
                "if ((gb3 == \"DoesNotExist\")) {\n" +
                "(not any0)} else {\n" +
                "(set_size(count_distinct) as signed<64> == gb4)}}})\n" +
                "}\n" +
                "\n" +
                "function agg1(g: Group<string, TRtmp7>):Tagg1 {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(count0 = agg_count_R(count0, 64'sd1))}\n" +
                ");\n" +
                "(Tagg1{.count = count0})\n" +
                "}\n" +
                "\n" +
                "function agg2(g: Group<(string, bigint, bigint, bigint), (TRnode_info, TRpod_info, Ttmp6)>):Tagg2 {\n" +
                "(var gb, var gb0, var gb1, var gb2) = group_key(g);\n" +
                "(var sum = 0: bigint);\n" +
                "(var sum0 = 0: bigint);\n" +
                "(var sum1 = 0: bigint);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var v1 = i.2);\n" +
                "(var incr = v1.cpu_request);\n" +
                "(sum = agg_sum_int_R(sum, incr));\n" +
                "(var incr0 = v1.memory_request);\n" +
                "(sum0 = agg_sum_int_R(sum0, incr0));\n" +
                "(var incr1 = v1.pods_request);\n" +
                "(sum1 = agg_sum_int_R(sum1, incr1))}\n" +
                ");\n" +
                "(Tagg2{.cpu_remaining = (gb0 - sum) as signed<64>,.memory_remaining = (gb1 - sum0) as signed<64>,.pods_remaining = (gb2 - sum1) as signed<64>})\n" +
                "}\n" +
                "\n" +
                "function agg3(g: Group<string, TRtmp12>):Tagg1 {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v6 = i;\n" +
                "(count0 = agg_count_R(count0, 64'sd1))}\n" +
                ");\n" +
                "(Tagg1{.count = count0})\n" +
                "}\n" +
                "\n" +
                "function agg4(g0: Group<(string, string, signed<64>), (Ttmp7, Ttmp7, TRtmp14, Ttmp9)>):Tagg0 {\n" +
                "(var gb3, var gb4, var gb5) = group_key(g0);\n" +
                "(var count2 = 64'sd0: signed<64>);\n" +
                "(for ((i0, _) in g0) {\n" +
                "var v1 = i0.0;\n" +
                "(var v14 = i0.1);\n" +
                "(var v13 = i0.2);\n" +
                "(var v15 = i0.3);\n" +
                "(count2 = agg_count_R(count2, 64'sd1))}\n" +
                ");\n" +
                "(Tagg0{.col = (count2 == gb5)})\n" +
                "}\n" +
                "\n" +
                "input relation Rnode_info[TRnode_info] primary key (row) (row.name)\n" +
                "input relation Rpod_info[TRpod_info] primary key (row) (row.pod_name)\n" +
                "input relation Rpod_ports_request[TRpod_ports_request]\n" +
                "input relation Rcontainer_host_ports[TRcontainer_host_ports]\n" +
                "input relation Rpod_node_selector_labels[TRpod_node_selector_labels]\n" +
                "input relation Rpod_affinity_match_expressions[TRpod_affinity_match_expressions]\n" +
                "input relation Rpod_anti_affinity_match_expressions[TRpod_anti_affinity_match_expressions]\n" +
                "input relation Rpod_labels[TRpod_labels]\n" +
                "input relation Rnode_labels[TRnode_labels]\n" +
                "input relation Rvolume_labels[TRvolume_labels]\n" +
                "input relation Rpod_by_service[TRpod_by_service]\n" +
                "input relation Rservice_affinity_labels[TRservice_affinity_labels]\n" +
                "input relation Rlabels_to_check_for_presence[TRlabels_to_check_for_presence]\n" +
                "input relation Rnode_taints[TRnode_taints]\n" +
                "input relation Rpod_tolerations[TRpod_tolerations]\n" +
                "input relation Rnode_images[TRnode_images]\n" +
                "input relation Rpod_images[TRpod_images]\n" +
                "output relation Rpods_to_assign_no_limit[TRtmp]\n" +
                "input relation Rbatch_size[TRbatch_size] primary key (row) (row.pendingPodsLimit)\n" +
                "output relation Rpods_to_assign[TRtmp]\n" +
                "output relation Rpods_with_port_requests[TRtmp1]\n" +
                "relation Rtmp2[Ttmp0]\n" +
                "relation Rtmp3[TRtmp3]\n" +
                "output relation Rpod_node_selector_matches[TRtmp3]\n" +
                "relation Rtmp4[Ttmp2]\n" +
                "relation Rtmp5[Ttmp3]\n" +
                "relation Rtmp6[TRtmp6]\n" +
                "output relation Rinter_pod_affinity_matches_inner[TRtmp6]\n" +
                "relation Roverinput[TRtmp7]\n" +
                "relation Rtmp8[TRtmp8]\n" +
                "relation Rover[TRtmp8]\n" +
                "output relation Rinter_pod_affinity_matches[TRtmp9]\n" +
                "relation Rtmp10[TRtmp10]\n" +
                "output relation Rspare_capacity_per_node[TRtmp10]\n" +
                "output relation Rnodes_that_have_tolerations[TRtmp11]\n" +
                "relation Roverinput0[TRtmp12]\n" +
                "relation Rtmp13[TRtmp8]\n" +
                "relation Rover0[TRtmp8]\n" +
                "relation Rtmp15[TRtmp14]\n" +
                "relation Rtmp16[Ttmp7]\n" +
                "relation Rtmp17[TRtmp3]\n" +
                "output relation Rpods_that_tolerate_node_taints[TRtmp3]\n" +
                "output relation Rassigned_pods[TRtmp18]\n" +
                "Rpods_to_assign_no_limit[v1] :- Rpod_info[v],unwrapBool(b_and_RN(((v.status == \"Pending\") and is_null(v.node_name)), s_eq_NR(v.schedulerName, \"dcm-scheduler\"))),var v0 = TRtmp{.pod_name = v.pod_name,.status = v.status,.controllable__node_name = v.node_name,.namespace = v.namespace,.cpu_request = v.cpu_request,.memory_request = v.memory_request,.ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp,.has_node_selector_labels = v.has_node_selector_labels,.has_pod_affinity_requirements = v.has_pod_affinity_requirements},var v1 = v0.\n" +
                "Rpods_to_assign[v0] :- Rpods_to_assign_no_limit[v],var v0 = v.\n" +
                "Rpods_with_port_requests[v3] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_ports_request[TRpod_ports_request{.pod_name = pod_name,.host_ip = host_ip,.host_port = host_port,.host_protocol = host_protocol}],var v1 = Ttmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.host_ip = host_ip,.host_port = host_port,.host_protocol = host_protocol},var v2 = TRtmp1{.controllable__node_name = v1.controllable__node_name,.host_port = v1.host_port,.host_ip = v1.host_ip,.host_protocol = v1.host_protocol},var v3 = v2.\n" +
                "Rtmp2[v3] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_node_selector_labels[TRpod_node_selector_labels{.pod_name = pod_name,.term = term,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value}],var v1 = Ttmp0{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.term = term,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value},var v3 = v1.\n" +
                "Rpod_node_selector_matches[v6] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_node_selector_labels[TRpod_node_selector_labels{.pod_name = pod_name,.term = term,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value}],var v1 = Ttmp0{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.term = term,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value},var v3 = v1,Rnode_labels[v2],unwrapBool(b_or_NR(b_or_NR(b_or_NR(b_and_RN(((v1.label_operator == \"In\") and (v1.label_key == v2.label_key)), s_eq_NR(v1.label_value, v2.label_value)), ((v1.label_operator == \"Exists\") and (v1.label_key == v2.label_key))), (v1.label_operator == \"NotIn\")), (v1.label_operator == \"DoesNotExist\"))),var v4 = Ttmp1{.pod_name = v1.pod_name,.status = v1.status,.controllable__node_name = v1.controllable__node_name,.namespace = v1.namespace,.cpu_request = v1.cpu_request,.memory_request = v1.memory_request,.ephemeral_storage_request = v1.ephemeral_storage_request,.pods_request = v1.pods_request,.owner_name = v1.owner_name,.creation_timestamp = v1.creation_timestamp,.has_node_selector_labels = v1.has_node_selector_labels,.has_pod_affinity_requirements = v1.has_pod_affinity_requirements,.term = v1.term,.match_expression = v1.match_expression,.num_match_expressions = v1.num_match_expressions,.label_key = v1.label_key,.label_operator = v1.label_operator,.label_value = v1.label_value,.node_name = v2.node_name,.label_key0 = v2.label_key,.label_value0 = v2.label_value},var gb = v4.pod_name,var gb0 = v4.node_name,var gb1 = v4.term,var gb2 = v4.label_operator,var gb3 = v4.num_match_expressions,var groupResult = (v1, v3, v2, v4).group_by((gb, gb0, gb1, gb2, gb3)),var aggResult = agg(groupResult),var v5 = TRtmp3{.pod_name = gb,.node_name = gb0},unwrapBool(aggResult.col),var v6 = v5.\n" +
                "Rtmp4[v3] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_affinity_match_expressions[TRpod_affinity_match_expressions{.pod_name = pod_name,.label_selector = label_selector,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value,.topology_key = topology_key}],var v1 = Ttmp2{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.label_selector = label_selector,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value,.topology_key = topology_key},var v3 = v1.\n" +
                "Rtmp5[v6] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_affinity_match_expressions[TRpod_affinity_match_expressions{.pod_name = pod_name,.label_selector = label_selector,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value,.topology_key = topology_key}],var v1 = Ttmp2{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.label_selector = label_selector,.match_expression = match_expression,.num_match_expressions = num_match_expressions,.label_key = label_key,.label_operator = label_operator,.label_value = label_value,.topology_key = topology_key},var v3 = v1,Rpod_labels[v2],((((((v1.label_operator == \"In\") and (v1.label_key == v2.label_key)) and (v1.label_value == v2.label_value)) or ((v1.label_operator == \"Exists\") and (v1.label_key == v2.label_key))) or (v1.label_operator == \"NotIn\")) or (v1.label_operator == \"DoesNotExist\")),var v4 = Ttmp3{.pod_name = v1.pod_name,.status = v1.status,.controllable__node_name = v1.controllable__node_name,.namespace = v1.namespace,.cpu_request = v1.cpu_request,.memory_request = v1.memory_request,.ephemeral_storage_request = v1.ephemeral_storage_request,.pods_request = v1.pods_request,.owner_name = v1.owner_name,.creation_timestamp = v1.creation_timestamp,.has_node_selector_labels = v1.has_node_selector_labels,.has_pod_affinity_requirements = v1.has_pod_affinity_requirements,.label_selector = v1.label_selector,.match_expression = v1.match_expression,.num_match_expressions = v1.num_match_expressions,.label_key = v1.label_key,.label_operator = v1.label_operator,.label_value = v1.label_value,.topology_key = v1.topology_key,.pod_name0 = v2.pod_name,.label_key0 = v2.label_key,.label_value0 = v2.label_value},var v6 = v4.\n" +
                "Rinter_pod_affinity_matches_inner[v9] :- Rtmp5[Ttmp3{.pod_name = pod_name2,.status = status0,.controllable__node_name = controllable__node_name0,.namespace = namespace0,.cpu_request = cpu_request0,.memory_request = memory_request0,.ephemeral_storage_request = ephemeral_storage_request0,.pods_request = pods_request0,.owner_name = owner_name0,.creation_timestamp = creation_timestamp0,.has_node_selector_labels = has_node_selector_labels0,.has_pod_affinity_requirements = has_pod_affinity_requirements0,.label_selector = label_selector0,.match_expression = match_expression0,.num_match_expressions = num_match_expressions0,.label_key = label_key1,.label_operator = label_operator0,.label_value = label_value1,.topology_key = topology_key0,.pod_name0 = pod_name00,.label_key0 = label_key00,.label_value0 = label_value00}],Rpod_info[TRpod_info{.pod_name = pod_name2,.status = status1,.node_name = node_name,.namespace = namespace1,.cpu_request = cpu_request1,.memory_request = memory_request1,.ephemeral_storage_request = ephemeral_storage_request1,.pods_request = pods_request1,.owner_name = owner_name1,.creation_timestamp = creation_timestamp1,.priority = priority,.schedulerName = schedulerName,.has_node_selector_labels = has_node_selector_labels1,.has_pod_affinity_requirements = has_pod_affinity_requirements1}],var v7 = Ttmp4{.pod_name = pod_name2,.status = status0,.controllable__node_name = controllable__node_name0,.namespace = namespace0,.cpu_request = cpu_request0,.memory_request = memory_request0,.ephemeral_storage_request = ephemeral_storage_request0,.pods_request = pods_request0,.owner_name = owner_name0,.creation_timestamp = creation_timestamp0,.has_node_selector_labels = has_node_selector_labels0,.has_pod_affinity_requirements = has_pod_affinity_requirements0,.label_selector = label_selector0,.match_expression = match_expression0,.num_match_expressions = num_match_expressions0,.label_key = label_key1,.label_operator = label_operator0,.label_value = label_value1,.topology_key = topology_key0,.pod_name0 = pod_name00,.label_key0 = label_key00,.label_value0 = label_value00,.status0 = status1,.node_name = node_name,.namespace0 = namespace1,.cpu_request0 = cpu_request1,.memory_request0 = memory_request1,.ephemeral_storage_request0 = ephemeral_storage_request1,.pods_request0 = pods_request1,.owner_name0 = owner_name1,.creation_timestamp0 = creation_timestamp1,.priority = priority,.schedulerName = schedulerName,.has_node_selector_labels0 = has_node_selector_labels1,.has_pod_affinity_requirements0 = has_pod_affinity_requirements1},var gb = v7.pod_name,var gb0 = v7.pod_name0,var gb1 = v7.label_selector,var gb2 = v7.topology_key,var gb3 = v7.label_operator,var gb4 = v7.num_match_expressions,var gb5 = v7.node_name,var groupResult = (v7).group_by((gb, gb0, gb1, gb2, gb3, gb4, gb5)),var aggResult = agg0(groupResult),var v8 = TRtmp6{.pod_name = gb,.matches = gb0,.node_name = gb5},aggResult.col,var v9 = v8.\n" +
                "Roverinput[v2] :- Rinter_pod_affinity_matches_inner[v0],var v1 = TRtmp7{.gb = v0.pod_name,.pod_name = v0.pod_name,.matches = v0.matches,.node_name = v0.node_name},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb0 = v3.gb,var groupResult = (v3).group_by((gb0)),var aggResult = agg1(groupResult),var v4 = TRtmp8{.gb = gb0,.count = aggResult.count},var v5 = v4.\n" +
                "Rinter_pod_affinity_matches[v10] :- Roverinput[TRtmp7{.gb = gb1,.pod_name = pod_name,.matches = matches,.node_name = node_name}],Rover[TRtmp8{.gb = gb1,.count = count1}],var v8 = Ttmp5{.gb = gb1,.pod_name = pod_name,.matches = matches,.node_name = node_name,.count = count1},var v9 = TRtmp9{.pod_name = v8.pod_name,.matches = v8.matches,.node_name = v8.node_name,.num_matches = v8.count},var v10 = v9.\n" +
                "Rspare_capacity_per_node[v3] :- Rnode_info[v],Rpod_info[v0],unwrapBool(b_and_NN(s_eq_NR(v0.node_name, v.name), s_neq_NR(v0.node_name, \"null\"))),var v1 = Ttmp6{.name = v.name,.unschedulable = v.unschedulable,.out_of_disk = v.out_of_disk,.memory_pressure = v.memory_pressure,.disk_pressure = v.disk_pressure,.pid_pressure = v.pid_pressure,.ready = v.ready,.network_unavailable = v.network_unavailable,.cpu_capacity = v.cpu_capacity,.memory_capacity = v.memory_capacity,.ephemeral_storage_capacity = v.ephemeral_storage_capacity,.pods_capacity = v.pods_capacity,.cpu_allocatable = v.cpu_allocatable,.memory_allocatable = v.memory_allocatable,.ephemeral_storage_allocatable = v.ephemeral_storage_allocatable,.pods_allocatable = v.pods_allocatable,.pod_name = v0.pod_name,.status = v0.status,.node_name = v0.node_name,.namespace = v0.namespace,.cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request,.pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp,.priority = v0.priority,.schedulerName = v0.schedulerName,.has_node_selector_labels = v0.has_node_selector_labels,.has_pod_affinity_requirements = v0.has_pod_affinity_requirements},var gb = v1.name,var gb0 = v1.cpu_allocatable,var gb1 = v1.memory_allocatable,var gb2 = v1.pods_allocatable,var groupResult = (v, v0, v1).group_by((gb, gb0, gb1, gb2)),var aggResult = agg2(groupResult),var v2 = TRtmp10{.name = gb,.cpu_remaining = aggResult.cpu_remaining,.memory_remaining = aggResult.memory_remaining,.pods_remaining = aggResult.pods_remaining},var v3 = v2.\n" +
                "Rnodes_that_have_tolerations[v1] :- Rnode_taints[v],var v0 = TRtmp11{.node_name = v.node_name},var v1 = v0.\n" +
                "Roverinput0[v5] :- Rnode_taints[v3],var v4 = TRtmp12{.gb = v3.node_name,.node_name = v3.node_name,.taint_key = v3.taint_key,.taint_value = v3.taint_value,.taint_effect = v3.taint_effect},var v5 = v4.\n" +
                "Rover0[v8] :- Roverinput0[v6],var gb0 = v6.gb,var groupResult = (v6).group_by((gb0)),var aggResult = agg3(groupResult),var v7 = TRtmp8{.gb = gb0,.count = aggResult.count},var v8 = v7.\n" +
                "Rtmp15[v13] :- Roverinput0[TRtmp12{.gb = gb1,.node_name = node_name,.taint_key = taint_key,.taint_value = taint_value,.taint_effect = taint_effect}],Rover0[TRtmp8{.gb = gb1,.count = count1}],var v11 = Ttmp8{.gb = gb1,.node_name = node_name,.taint_key = taint_key,.taint_value = taint_value,.taint_effect = taint_effect,.count = count1},var v12 = TRtmp14{.node_name = v11.node_name,.taint_key = v11.taint_key,.taint_value = v11.taint_value,.taint_effect = v11.taint_effect,.num_taints = v11.count},var v13 = v12.\n" +
                "Rtmp16[v14] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_tolerations[TRpod_tolerations{.pod_name = pod_name,.tolerations_key = tolerations_key,.tolerations_value = tolerations_value,.tolerations_effect = tolerations_effect,.tolerations_operator = tolerations_operator}],var v1 = Ttmp7{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.tolerations_key = tolerations_key,.tolerations_value = tolerations_value,.tolerations_effect = tolerations_effect,.tolerations_operator = tolerations_operator},var v14 = v1.\n" +
                "Rpods_that_tolerate_node_taints[v17] :- Rpods_to_assign[TRtmp{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements}],Rpod_tolerations[TRpod_tolerations{.pod_name = pod_name,.tolerations_key = tolerations_key,.tolerations_value = tolerations_value,.tolerations_effect = tolerations_effect,.tolerations_operator = tolerations_operator}],var v1 = Ttmp7{.pod_name = pod_name,.status = status,.controllable__node_name = controllable__node_name,.namespace = namespace,.cpu_request = cpu_request,.memory_request = memory_request,.ephemeral_storage_request = ephemeral_storage_request,.pods_request = pods_request,.owner_name = owner_name,.creation_timestamp = creation_timestamp,.has_node_selector_labels = has_node_selector_labels,.has_pod_affinity_requirements = has_pod_affinity_requirements,.tolerations_key = tolerations_key,.tolerations_value = tolerations_value,.tolerations_effect = tolerations_effect,.tolerations_operator = tolerations_operator},var v14 = v1,Rtmp15[v13],unwrapBool(b_and_NN(b_and_NN(s_eq_NR(v1.tolerations_key, v13.taint_key), b_or_NN(s_eq_NN(v1.tolerations_effect, None{}: Option<string>), s_eq_NR(v1.tolerations_effect, v13.taint_effect))), b_or_NN(s_eq_NR(v1.tolerations_operator, \"Exists\"), s_eq_NN(v1.tolerations_value, v13.taint_value)))),var v15 = Ttmp9{.pod_name = v1.pod_name,.status = v1.status,.controllable__node_name = v1.controllable__node_name,.namespace = v1.namespace,.cpu_request = v1.cpu_request,.memory_request = v1.memory_request,.ephemeral_storage_request = v1.ephemeral_storage_request,.pods_request = v1.pods_request,.owner_name = v1.owner_name,.creation_timestamp = v1.creation_timestamp,.has_node_selector_labels = v1.has_node_selector_labels,.has_pod_affinity_requirements = v1.has_pod_affinity_requirements,.tolerations_key = v1.tolerations_key,.tolerations_value = v1.tolerations_value,.tolerations_effect = v1.tolerations_effect,.tolerations_operator = v1.tolerations_operator,.node_name = v13.node_name,.taint_key = v13.taint_key,.taint_value = v13.taint_value,.taint_effect = v13.taint_effect,.num_taints = v13.num_taints},var gb3 = v15.pod_name,var gb4 = v15.node_name,var gb5 = v15.num_taints,var groupResult0 = (v1, v14, v13, v15).group_by((gb3, gb4, gb5)),var aggResult0 = agg4(groupResult0),var v16 = TRtmp3{.pod_name = gb3,.node_name = gb4},aggResult0.col,var v17 = v16.\n" +
                "Rassigned_pods[v1] :- Rpod_info[v],(not is_null(v.node_name)),var v0 = TRtmp18{.pod_name = v.pod_name,.status = v.status,.node_name = v.node_name,.namespace = v.namespace,.cpu_request = v.cpu_request,.memory_request = v.memory_request,.ephemeral_storage_request = v.ephemeral_storage_request,.pods_request = v.pods_request,.owner_name = v.owner_name,.creation_timestamp = v.creation_timestamp,.has_node_selector_labels = v.has_node_selector_labels,.has_pod_affinity_requirements = v.has_pod_affinity_requirements},var v1 = v0.";
        Assert.assertEquals(expected, p);
        this.compiledDDlog(p);
    }

    @Test
    public void testFullSchemaCompilation() {
        testFileCompilation("/weave.sql");
    }

    @Test
    public void testTableOnlySchemaCompilation() {
        testFileCompilation("/weave_tables.sql");
    }

    @Test
    public void testSingleTableCompilation() {
        testFileCompilation("/weave_minimal.sql");
    }
}
