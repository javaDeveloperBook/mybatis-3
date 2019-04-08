drop table t_users if exists;

create table t_users (
  id int,
  name varchar(20)
);

insert into t_users values(1, 'User1');
insert into t_users values(2, 'User2');
insert into t_users values(3, 'User3');
insert into t_users values(4, 'User4');
insert into t_users values(5, 'User5');