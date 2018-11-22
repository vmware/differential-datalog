echo Compile ovn-nb.ovsschema
ovsdb2ddlog -f ovn-nb.ovsschema  > OVN_Northbound.dl
echo Compile ovn-sb.ovsschema
ovsdb2ddlog -f ovn-sb.ovsschema \
            -o SB_Global        \
            -o Logical_Flow     \
            -o Multicast_Group  \
            -o Meter            \
            -o Meter_Band       \
            -o Datapath_Binding \
            -o Port_Binding     \
            -o Gateway_Chassis  \
            -o Port_Group       \
            -o MAC_Binding      \
            -o DHCP_Options     \
            -o DHCPv6_Options   \
            -o Address_Set      \
            -o DNS              \
            -o RBAC_Role        \
            -o RBAC_Permission  \
            -p Datapath_Binding \
            -p Port_Binding     \
            -p Datapath_Binding     \
            --ro Port_Binding.chassis       \
            -k Multicast_Group.datapath     \
            -k Multicast_Group.name         \
            -k Multicast_Group.tunnel_key   \
            -k Port_Binding.logical_port    \
            -k DNS.external_ids             \
            -k Datapath_Binding.external_ids\
            -k RBAC_Role.name               \
            -k Address_Set.name             \
            -k Port_Group.name              \
            -k Meter.name                   \
            > OVN_Southbound.dl
echo Compile ovn_northd.dl
ddlog -i ovn_northd.dl -L../../lib
pushd ovn_northd_ddlog
echo Running cargo build
RUSTFLAGS='-L ../../../../ovs/ovn/lib/.libs -L ../../../../ovs/lib/.libs -lssl -lcrypto' cargo build
popd
