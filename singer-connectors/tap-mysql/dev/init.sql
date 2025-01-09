# configure replication user
grant replication client on *.* to 'replication_user'@'%';
grant replication slave on *.* to 'replication_user'@'%';
flush privileges;

use tap_mysql_test;

# create objects
create table r1 (
    i1 int auto_increment primary key,
    c1 varchar(100),
    d1 datetime default current_timestamp()
);

select * from r1;

insert into r1 (c1) values ('#1'),('#2'),('#3'),('#4'),('#5'),('#6'),('#7');
insert into r1 (c1) values ('#8'),('#9'),('#10'),('#11'),('#12'),('#13'),('#14');
insert into r1 (c1) values ('#15'),('#16'),('#17'),('#18');

update r1 set c1=concat(c1, '- updated 1') where i1 < 10;

create table r2 (
    i2 int primary key,
    d2 datetime
) ;
insert into r2 (i2, d2) values (1, now()), (2, now()), (3, now()), (4, now());

update r1 set c1=concat(c1, '- update 2') where i1 >= 10;

select * from r2;

delete from r1 where i1 < 4;

drop table r2;

alter table r1 add column b1 bool default False;
insert into r1 (c1, b1) values ('#8', True);
