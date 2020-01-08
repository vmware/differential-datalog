package ddlog;

import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import org.junit.Assert;
import org.junit.Test;

public class WeaveTests {
    @Test
    public void testWeave() {
        Translator t = new Translator(null);
        String node_info = "create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null /*primary key*/,\n" +
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
        DDlogIRNode create = t.translateSqlStatement(node_info);
        Assert.assertNotNull(create);

        String pod_info =
                "create table pod_info\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null /*primary key*/,\n" +
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
        create = t.translateSqlStatement(pod_info);
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
                        "  /*,foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_ports_request);
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
        create = t.translateSqlStatement(container_host_ports);
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
        create = t.translateSqlStatement(pod_node_selector_labels);
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
        create = t.translateSqlStatement(pod_affinity_match_expressions);
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
        create = t.translateSqlStatement(pod_anti_affinity_match_expressions);
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
        create = t.translateSqlStatement(pod_labels);
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
        create = t.translateSqlStatement(node_labels);
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
        create = t.translateSqlStatement(volume_labels);
        Assert.assertNotNull(create);

        String pod_by_service =
                "-- For pods that have ports exposed\n" +
                        "create table pod_by_service\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  service_name varchar(100) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_by_service);
        Assert.assertNotNull(create);

        String service_affinity_labels =
                "-- Service affinity labels\n" +
                        "create table service_affinity_labels\n" +
                        "(\n" +
                        "  label_key varchar(100) not null\n" +
                        ")";
        create = t.translateSqlStatement(service_affinity_labels);
        Assert.assertNotNull(create);

        String labels_to_check_for_presence =
                "-- Labels present on node\n" +
                        "create table labels_to_check_for_presence\n" +
                        "(\n" +
                        "  label_key varchar(100) not null,\n" +
                        "  present boolean not null\n" +
                        ")";
        create = t.translateSqlStatement(labels_to_check_for_presence);
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
        create = t.translateSqlStatement(node_taints);
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
        create = t.translateSqlStatement(pod_taints);
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
        create = t.translateSqlStatement(node_images);
        Assert.assertNotNull(create);

        String pod_images =
                "-- Tracks the container images required by each pod\n" +
                        "create table pod_images\n" +
                        "(\n" +
                        "  pod_name varchar(100) not null,\n" +
                        "  image_name varchar(200) not null/*,\n" +
                        "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                        ")";
        create = t.translateSqlStatement(pod_images);
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
                        "/*order by creation_timestamp*/\n";
        create = t.translateSqlStatement(pods_to_assign_no_limit);
        Assert.assertNotNull(create);

        String batch_size =
                "\n" +
                        "-- This view is updated dynamically to change the limit. This\n" +
                        "-- pattern is required because there is no clean way to enforce\n" +
                        "-- a dynamic \"LIMIT\" clause.\n" +
                        "create table batch_size\n" +
                        "(\n" +
                        "  pendingPodsLimit integer not null /*primary key*/\n" +
                        ")";
        create = t.translateSqlStatement(batch_size);
        Assert.assertNotNull(create);

        String pods_to_assign =
                "create view pods_to_assign as\n" +
                        "select DISTINCT * from pods_to_assign_no_limit /*limit 100*/\n";
        create = t.translateSqlStatement(pods_to_assign);
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
        create = t.translateSqlStatement(pods_with_port_request);
        Assert.assertNotNull(create);

        /*
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
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

        // indexes are ignored
        String indexes =
                "create index pod_info_idx on pod_info (status, node_name);\n" +
                "create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_name);\n" +
                "create index node_labels_idx on node_labels (label_key, label_value);\n" +
                "create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);\n" +
                "create index pod_labels_idx on pod_labels (label_key, label_value);\n";
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

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
        create = t.translateSqlStatement(inter_pod_affinity);
        Assert.assertNotNull(create);

        String inter_pod_affinity_matches =
                "create view inter_pod_affinity_matches as\n" +
                "select *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner";
        create = t.translateSqlStatement(inter_pod_affinity_matches);
        Assert.assertNotNull(create);

        String spare_capacity =
                "-- Spare capacity\n" +
                "create view spare_capacity_per_node as\n" +
                "select node_info.name as name,\n" +
                "       cast(node_info.cpu_allocatable - sum(pod_info.cpu_request) as integer) as cpu_remaining,\n" +
                "       cast(node_info.memory_allocatable - sum(pod_info.memory_request) as integer) as memory_remaining,\n" +
                "       cast(node_info.pods_allocatable - sum(pod_info.pods_request) as integer) as pods_remaining\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'\n" +
                "group by node_info.name, node_info.cpu_allocatable,\n" +
                "         node_info.memory_allocatable, node_info.pods_allocatable";
        create = t.translateSqlStatement(spare_capacity);
        Assert.assertNotNull(create);

        String view_pods_that_tolerate_node_taints =
                "-- Taints and tolerations\n" +
                "create view pods_that_tolerate_node_taints as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       A.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_tolerations\n" +
                "     on pods_to_assign.pod_name = pod_tolerations.pod_name\n" +
                "join (select *, count(*) over (partition by node_name) as num_taints from node_taints) as A\n" +
                "     on pod_tolerations.tolerations_key = A.taint_key\n" +
                "     and (pod_tolerations.tolerations_effect = null\n" +
                "          or pod_tolerations.tolerations_effect = A.taint_effect)\n" +
                "     and (pod_tolerations.tolerations_operator = 'Exists'\n" +
                "          or pod_tolerations.tolerations_value = A.taint_value)\n" +
                "group by pod_tolerations.pod_name, A.node_name, A.num_taints\n" +
                "having count(*) = A.num_taints";
        create = t.translateSqlStatement(view_pods_that_tolerate_node_taints);
        Assert.assertNotNull(create);
        */

        String nodes_that_have_tollerations =
                "create view nodes_that_have_tolerations as\n" +
                        "select distinct node_name from node_taints";
        create = t.translateSqlStatement(nodes_that_have_tollerations);
        Assert.assertNotNull(create);

        DDlogProgram program = t.getDDlogProgram();
        String p = program.toString();
        Assert.assertNotNull(p);
        String expected = "import sql\nimport sqlop\n" + "\n" +
                "typedef Tnode_info = Tnode_info{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, memory_capacity:bigint, ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint}\n" +
                "typedef Tpod_info = Tpod_info{pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "typedef Tpod_ports_request = Tpod_ports_request{pod_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef Tcontainer_host_ports = Tcontainer_host_ports{pod_name:string, node_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef Tpod_node_selector_labels = Tpod_node_selector_labels{pod_name:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:Option<string>}\n" +
                "typedef Tpod_affinity_match_expressions = Tpod_affinity_match_expressions{pod_name:string, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
                "typedef Tpod_anti_affinity_match_expressions = Tpod_anti_affinity_match_expressions{pod_name:string, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
                "typedef Tpod_labels = Tpod_labels{pod_name:string, label_key:string, label_value:string}\n" +
                "typedef Tnode_labels = Tnode_labels{node_name:string, label_key:string, label_value:string}\n" +
                "typedef Tvolume_labels = Tvolume_labels{volume_name:string, pod_name:string, label_key:string, label_value:string}\n" +
                "typedef Tpod_by_service = Tpod_by_service{pod_name:string, service_name:string}\n" +
                "typedef Tservice_affinity_labels = Tservice_affinity_labels{label_key:string}\n" +
                "typedef Tlabels_to_check_for_presence = Tlabels_to_check_for_presence{label_key:string, present:bool}\n" +
                "typedef Tnode_taints = Tnode_taints{node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string}\n" +
                "typedef Tpod_tolerations = Tpod_tolerations{pod_name:string, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>}\n" +
                "typedef Tnode_images = Tnode_images{node_name:string, image_name:string, image_size:bigint}\n" +
                "typedef Tpod_images = Tpod_images{pod_name:string, image_name:string}\n" +
                "typedef Ttmp0 = Ttmp0{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
                "typedef Tbatch_size = Tbatch_size{pendingPodsLimit:signed<64>}\n" +
                "typedef Ttmp2 = Ttmp2{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
                "typedef Ttmp3 = Ttmp3{controllable__node_name:Option<string>, host_port:signed<64>, host_ip:string, host_protocol:string}\n" +
                "typedef Ttmp4 = Ttmp4{node_name:string}\n" +
                "\n" +
                "input relation Rnode_info[Tnode_info]\n" +
                "input relation Rpod_info[Tpod_info]\n" +
                "input relation Rpod_ports_request[Tpod_ports_request]\n" +
                "input relation Rcontainer_host_ports[Tcontainer_host_ports]\n" +
                "input relation Rpod_node_selector_labels[Tpod_node_selector_labels]\n" +
                "input relation Rpod_affinity_match_expressions[Tpod_affinity_match_expressions]\n" +
                "input relation Rpod_anti_affinity_match_expressions[Tpod_anti_affinity_match_expressions]\n" +
                "input relation Rpod_labels[Tpod_labels]\n" +
                "input relation Rnode_labels[Tnode_labels]\n" +
                "input relation Rvolume_labels[Tvolume_labels]\n" +
                "input relation Rpod_by_service[Tpod_by_service]\n" +
                "input relation Rservice_affinity_labels[Tservice_affinity_labels]\n" +
                "input relation Rlabels_to_check_for_presence[Tlabels_to_check_for_presence]\n" +
                "input relation Rnode_taints[Tnode_taints]\n" +
                "input relation Rpod_tolerations[Tpod_tolerations]\n" +
                "input relation Rnode_images[Tnode_images]\n" +
                "input relation Rpod_images[Tpod_images]\n" +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rpods_to_assign_no_limit[Ttmp0]\n" +
                "input relation Rbatch_size[Tbatch_size]\n" +
                "output relation Rpods_to_assign[Ttmp0]\n" +
                "relation Rtmp3[Ttmp3]\n" +
                "output relation Rpods_with_port_requests[Ttmp3]\n" +
                "relation Rtmp4[Ttmp4]\n" +
                "output relation Rnodes_that_have_tolerations[Ttmp4]\n" +
                "Rpods_to_assign_no_limit[v2] :- Rpod_info[v0],unwrapBool(b_and_RN(((v0.status == \"Pending\") and isNull(v0.node_name)), s_eq_NR(v0.schedulerName, \"dcm-scheduler\"))),var v1 = Ttmp0{.pod_name = v0.pod_name,.status = v0.status,.controllable__node_name = v0.node_name,.namespace = v0.namespace,.cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request,.pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp,.has_node_selector_labels = v0.has_node_selector_labels,.has_pod_affinity_requirements = v0.has_pod_affinity_requirements},var v2 = v1.\n" +
                "Rpods_to_assign[v1] :- Rpods_to_assign_no_limit[v0],var v1 = v0.\n" +
                "Rpods_with_port_requests[v4] :- Rpods_to_assign[v0],Rpod_ports_request[v1],(v1.pod_name == v0.pod_name),true,var v2 = Ttmp2{.pod_name = v0.pod_name,.status = v0.status,.controllable__node_name = v0.controllable__node_name,.namespace = v0.namespace,.cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request,.pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp,.has_node_selector_labels = v0.has_node_selector_labels,.has_pod_affinity_requirements = v0.has_pod_affinity_requirements,.pod_name0 = v1.pod_name,.host_ip = v1.host_ip,.host_port = v1.host_port,.host_protocol = v1.host_protocol},var v3 = Ttmp3{.controllable__node_name = v0.controllable__node_name,.host_port = v1.host_port,.host_ip = v1.host_ip,.host_protocol = v1.host_protocol},var v4 = v3.\n" +
                "Rnodes_that_have_tolerations[v2] :- Rnode_taints[v0],var v1 = Ttmp4{.node_name = v0.node_name},var v2 = v1.";
        Assert.assertEquals(expected, p);
    }
}
