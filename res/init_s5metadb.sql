-- select convert(SUBSTRING_INDEX(version(), '.', 1), integer)  into @majorv;
create database s5 ;
use s5;


DROP PROCEDURE IF EXISTS check_db_ver;
-- DELIMITER $$
-- CREATE PROCEDURE check_db_ver()
-- BEGIN
-- 	DECLARE majorv int;
-- 	select convert(SUBSTRING_INDEX(version(), '.', 1), integer)  into majorv;
-- 	if majorv < 11 then
-- 		\! echo "Need MariaDB version 10.4 or higher";
-- 		exit;
-- 	end if
-- END
-- $$
-- DELIMITER ;
-- call check_db_ver();


drop view if exists v_id;
drop view  if exists v_store_use;

drop view  if exists v_store_alloc_size;
drop view  if exists v_store_total_size;
drop view  if exists v_store_free_size;
drop view  if exists v_tray_alloc_size;
drop view  if exists v_tray_total_size;
drop view  if exists v_tray_free_size;
drop view  if exists v_primary_count;
drop view  if exists v_primary_s5store_id;
drop view  if exists v_replica_ext;
drop table  if exists t_volume;
drop table  if exists t_replica;
drop table  if exists t_quotaset;
drop table  if exists t_tenant;
drop table  if exists t_nic;
drop table  if exists t_tray;
drop table  if exists t_task_journal;
drop table  if exists t_seq_gen;
drop table  if exists t_store;
drop table  if exists t_snapshot;
-- 'auth' column of 'tenant' describes access permission level of current tenant, 0 indicates normal user, 1 indicates administrator, -1 invalid tenant
-- car id from 0 ~ 63 is reserved for special usage, and will not be set to rge.
-- 
create table t_tenant(
	id integer primary key AUTO_INCREMENT,
	car_id integer ,
	name varchar(96) unique not null, 
	pass_wd varchar(256) not null, 
	auth int not null, 
	size bigint not null, 
	iops int not null, 
	cbs int not null, 
	bw int not null)ENGINE=InnoDB AUTO_INCREMENT=65;

insert into t_tenant(id, car_id, name, pass_wd, auth, size, iops, cbs, bw) values(1, 0, 'tenant_default', '123456', -1, 0, 0, 0, 0);
insert into t_tenant(id, car_id, name, pass_wd, auth, size, iops, cbs, bw) values(2, 0, 'system_sp_tenant', '123456', -1, 0, 0, 0, 0);

-- init administrator (id for administrator starts from 32 to 63)
insert into t_tenant(id, car_id, name, pass_wd, auth, size, iops, cbs, bw) values(32, -1, 'admin', '123456', 1, 0, 0, 0, 0);

create table t_quotaset(
	id integer primary key not null, 
	car_id integer not null, 
	name varchar(96) not null, 
	iops int not null, 
	cbs int not null, 
	bw int not null, 
	tenant_id integer not null, 
	foreign key (tenant_id) references t_tenant(id));

insert into t_quotaset(id, car_id, name, iops, cbs, bw, tenant_id) values(65, 1, 'quotaset_default', 0, 0, 0, 1);

create table t_store(
	id integer primary key, 
	name varchar(96) , 
	sn varchar(128) , 
	model varchar(128) , 
	mngt_ip varchar(32) not null,
	status varchar(16) not null
	);

-- unique(mngt_ip,status)  when deployed in k8s, mngt_ip may change when pod moved

-- 'access' describes access permission property of volume, 1 ---'00 01' owner read-only, 3 --- '00 11' owner read-write, 5 --- '01 01' all read, 7 --- '01 11' all-read owner-write,
-- 15 --- '11 11' all read-write
create table t_volume(
	id bigint primary key not null, 
	name varchar(96) not null, 
	size bigint not null, 
	iops int not null , 
	cbs int not null, 
	bw int not null, 
	tenant_id integer not null, 
	quotaset_id integer, 
	status varchar(16), 
	meta_ver integer default 0,
	features integer default 0,
	exposed integer default 0,
	rep_count integer default 1,
	snap_seq integer default 0,
	shard_size bigint default (64<<30),
	status_time datetime not null default current_timestamp on update current_timestamp,
	foreign key (tenant_id) references t_tenant(id)
	);


create view v_id as select id from t_tenant union all select id from t_volume union all select id from t_quotaset;

create table if not exists t_shard (
    id bigint unsigned not null primary key,
    volume_id bigint unsigned not null,
    shard_index bigint unsigned not null,
    primary_rep_index bigint unsigned not null,
    status char(16),
	status_time datetime not null default current_timestamp on update current_timestamp,
	index(volume_id)
);




create table t_port(
	ip_addr varchar(16) ,
	store_id int,
	purpose int, -- normal/replicating access 
	status varchar(16) not null,
	primary key(ip_addr, purpose)
	);

create table t_tray(
	uuid varchar(64) primary key ,
	device varchar(96) not null, 
	status varchar(16) not null, 
	raw_capacity bigint not null,
	object_size bigint not null,
	store_id integer not null,
	foreign key (store_id) references t_store(id));
create table t_shared_disk(
	uuid varchar(64) primary key ,
	status varchar(16) not null, 
	raw_capacity bigint not null,
	object_size bigint not null,
	coowner    varchar(64)
	);

create table t_replica(
	id bigint primary key not null ,
	replica_index int not null,
	volume_id bigint  not null,
	shard_id bigint not null,
	store_id integer not null,
	tray_uuid	varchar(64) not null,
	status_time datetime not null default current_timestamp on update current_timestamp,
	status varchar(16));
create table t_snapshot(
    id bigint primary key AUTO_INCREMENT,
    volume_id bigint not null,
    snap_seq integer not null,
    name varchar(96) not null,
    size bigint not null, -- size of volume when snapshot created
    created datetime not null default current_timestamp
);
create view v_store_alloc_size as  select store_id, sum(t_volume.shard_size) as alloc_size from t_volume, t_replica where t_volume.id=t_replica.volume_id group by t_replica.store_id;

create view v_store_total_size as  select s.id as store_id, sum(t.raw_capacity) as total_size from t_tray as t, t_store as s where t.status="OK" and t.store_id=s.id group by store_id;

create view v_store_free_size as select t.store_id, t.total_size, COALESCE(a.alloc_size,0) as alloc_size , t.total_size-COALESCE(a.alloc_size,0) as free_size , t_store.status  from t_store, v_store_total_size as t left join v_store_alloc_size as a on t.store_id=a.store_id where t_store.id=t.store_id order by free_size desc;

create view v_tray_alloc_size as select  t_replica.store_id as store_id, tray_uuid, sum(t_volume.shard_size) as alloc_size from t_volume, t_replica where t_volume.id = t_replica.volume_id group by t_replica.tray_uuid , t_replica.store_id;	
create view v_tray_total_size as select store_id, uuid as tray_uuid, raw_capacity as total_size, status from t_tray;
create view v_tray_free_size as select t.store_id as store_id, t.tray_uuid as tray_uuid, t.total_size as total_size,
 COALESCE(a.alloc_size,0) as alloc_size , t.total_size-COALESCE(a.alloc_size,0) as free_size, t.status as status from v_tray_total_size as t left join v_tray_alloc_size as a on t.store_id=a.store_id and t.tray_uuid=a.tray_uuid order by free_size desc;
-- select store_id, tray_uuid, max(free_size) from v_tray_free_size group by store_id;

create view v_replica_ext as
select v.id as volume_id, v.name as volume_name, s.id as shard_id, s.shard_index as shard_index, r.id as replica_id,
t.id as tenant_id, r.replica_index as replica_index, r.status_time as status_time,
if(s.primary_rep_index=r.replica_index, 1, 0) as is_primary, r.store_id, r.tray_uuid, r.status,
(select group_concat(p.ip_addr) from t_port as p where p.store_id=r.store_id and p.purpose=0) data_ports,
(select group_concat(p.ip_addr) from t_port as p where p.store_id=r.store_id and p.purpose=1) rep_ports
from t_volume as v, t_shard as s, t_replica as r, t_tenant as t  where v.id=s.volume_id and s.id=r.shard_id and t.id=v.tenant_id;



DROP SEQUENCE IF EXISTS seq_gen;
CREATE SEQUENCE seq_gen START WITH 66 INCREMENT BY 1;

DROP TRIGGER IF EXISTS update_vol_meta;
DELIMITER $$
CREATE TRIGGER update_vol_meta AFTER UPDATE ON t_replica
FOR EACH ROW
BEGIN
    IF new.status != old.status THEN
        update t_volume set meta_ver=meta_ver+1 where id=new.volume_id;
    END IF;
END;
$$
DELIMITER ;