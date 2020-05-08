create table node_info
(
  name varchar(36) not null,
  unschedulable boolean not null,
  out_of_disk boolean not null,
  memory_pressure boolean not null,
  disk_pressure boolean not null,
  pid_pressure boolean not null,
  ready boolean not null,
  network_unavailable boolean not null,
  cpu_capacity bigint not null,
  memory_capacity bigint not null,
  ephemeral_storage_capacity bigint not null,
  pods_capacity bigint not null,
  cpu_allocatable bigint not null,
  memory_allocatable bigint not null,
  ephemeral_storage_allocatable bigint not null,
  pods_allocatable bigint not null
);

create table pod_info
(
  pod_name varchar(100) not null,
  status varchar(36) not null,
  node_name varchar(36),
  namespace varchar(100) not null,
  cpu_request bigint not null,
  memory_request bigint not null,
  ephemeral_storage_request bigint not null,
  pods_request bigint not null,
  owner_name varchar(100) not null,
  creation_timestamp varchar(100) not null,
  priority integer not null,
  schedulerName varchar(50),
  has_node_selector_labels boolean not null,
  has_pod_affinity_requirements boolean not null,
  has_pod_anti_affinity_requirements boolean not null,
  equivalence_class bigint not null,
  qos_class varchar(10) not null
);

-- This table tracks the "ContainerPorts" fields of each pod.
-- It is used to enforce the PodFitsHostPorts constraint.
create table pod_ports_request
(
  pod_name varchar(100) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null
);

-- This table tracks the set of hostports in use at each node.
-- Also used to enforce the PodFitsHostPorts constraint.
create table container_host_ports
(
  pod_name varchar(100) not null,
  node_name varchar(36) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null
);

-- Tracks the set of node selector labels per pod.
create table pod_node_selector_labels
(
  pod_name varchar(100) not null,
  term integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(12) not null,
  label_value varchar(36)
);

-- Tracks the set of pod affinity match expressions.
create table pod_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_selector integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(30) not null,
  label_value varchar(36) not null,
  topology_key varchar(100) not null
);

-- Tracks the set of pod anti-affinity match expressions.
create table pod_anti_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_selector integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(30) not null,
  label_value varchar(36) not null,
  topology_key varchar(100) not null
);


-- Tracks the set of labels per pod, and indicates if
-- any of them are also node selector labels
create table pod_labels
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null
);

-- Tracks the set of labels per node
create table node_labels
(
  node_name varchar(36) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null
);

-- Volume labels
create table volume_labels
(
  volume_name varchar(36) not null,
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null
);

-- For pods that have ports exposed
create table pod_by_service
(
  pod_name varchar(100) not null,
  service_name varchar(100) not null
);

-- Service affinity labels
create table service_affinity_labels
(
  label_key varchar(100) not null
);


-- Labels present on node
create table labels_to_check_for_presence
(
  label_key varchar(100) not null,
  present boolean not null
);

-- Node taints
create table node_taints
(
  node_name varchar(36) not null,
  taint_key varchar(100) not null,
  taint_value varchar(100),
  taint_effect varchar(100) not null
);

-- Pod taints.
create table pod_tolerations
(
  pod_name varchar(100) not null,
  tolerations_key varchar(100),
  tolerations_value varchar(100),
  tolerations_effect varchar(100),
  tolerations_operator varchar(100)
);

-- Tracks the set of node images that are already
-- available at a node
create table node_images
(
  node_name varchar(36) not null,
  image_name varchar(200) not null,
  image_size bigint not null
);

-- Tracks the container images required by each pod
create table pod_images
(
  pod_name varchar(100) not null,
  image_name varchar(200) not null
);