select * from via.networks where id >= 99989 and id < 100000;

select * from via.links where network_id >= 99989 and network_id < 100000;
select * from via.link_lane_offset where network_id >= 99989 and network_id < 100000;
select * from via.link_lanes where network_id >= 99989 and network_id < 100000;
select * from via.link_names where network_id >= 99989 and network_id < 100000;
select * from via.link_type_det where network_id >= 99989 and network_id < 100000;

select * from via.nodes where network_id >= 99989 and network_id < 100000;
select * from via.node_names where network_id >= 99989 and network_id < 100000;
select * from via.node_type_det where network_id >= 99989 and network_id < 100000;

select * from via.demand_sets where id >= 99989 and id < 100000;
select * from via.demand_profs where id >= 99989 and id < 100000;
select * from via.demands where id >= 99989 and id < 100000;

select * from via.fund_diag_sets where id >= 99989 and id < 100000;
select * from via.fund_diag_profs where id >= 99989 and id < 100000;
select * from via.fund_diagrams where fund_diag_prof_id >= 99989 and fund_diag_prof_id < 100000;

select * from via.split_ratio_sets where id >= 99989 and id < 100000;
select * from via.split_ratio_profs where id >= 99989 and id < 100000;
select * from via.split_ratios where split_ratio_prof_id >= 99989 and split_ratio_prof_id < 100000;

select * from via.sensor_sets where id >= 99989 and id < 100000;
select * from via.sensors where sensor_set_id >= 99989 and sensor_set_id < 100000;

select * from via.scenarios where id >= 99989 and id < 100000;

select * from via.projects where id >= 99989 and id < 100000;
