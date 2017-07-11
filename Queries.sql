set hive.exec.reducers.bytes.per.reducer= 1000;
set hive.exec.reducers.max= 1000;
set mapreduce.job.reduces= 1000;
 

###############Creating MSIuser_id ###############

drop table if exists Msiuser_id;
create table Msiuser_id (msisdn bigint, user_id string);
insert into Msiuser_id
select msisdn, id from hike.users;


########### Creating Final Names ########################


drop table if exists temp;
create table temp (msisdn bigint, name string );

insert into temp 
select
contact_msisdn, contact_name from 
hike.addressbook_main_snapshot where contact_msisdn is not NULL ;

drop table if exists AllAddressbookName;
create table AllAddressbookName(user_id string, name string );
insert into AllAddressbookName
select 
msiuser_id.user_id,
temp.name
from msiuser_id inner join temp on
msiuser_id.msisdn = temp.msisdn;


drop table if exists AddressbookName;
create table AddressbookName(user_id string, name string );
insert into AllAddressbookName
select * from AllAddressbookName where user_id not in (select user_id from religion);

drop table if exists temp;
create table temp ( user_id string, name string, count int  );
insert into temp 
select user_id, name, count(user_id) from AddressbookName group by user_id, name ;

drop table if exists temp1;
create table temp1 ( user_id string, Maxcount string );
insert into temp1
select user_id, max(count) from temp temp;

drop table if exists temp2;
create table temp2( user_id string, name string, count int  );
insert into temp2 
select * from temp where 
concat(temp.user_id, temp.count) in (select concat(temp1.user_id, temp1.Maxcount ) from temp1 );

drop table if exists finalnames;
create table finalnames(user_id string, name string, count string);
insert into finalnames select * from temp2 
where name is not NULL;




______________________________________________________


drop table if exists temp;

create table temp(user_id string, name string, religion string, count int );
insert into temp 
select 
finalnames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from finalnames inner join religiondictionary 
on
lower(finalnames.name) = lower( religiondictionary.name );

insert into Religion
select temp.*, '1' from temp;

drop table if exists temp;
create table temp (user_id string, name string );

insert into temp 
select user_id, name from finalnames where user_id not in (select user_id from religion);

drop table if exists SplitedNames;
CREATE table SplitedNames(user_id string , name1 string, name2 string, name3 string );

insert into SplitedNames
select user_id, 
split( name, ' ')[0],
split( name, ' ')[1],
split( name, ' ')[2]
from temp;

drop table if exists temp;
create table temp(user_id string, name string, religion string, count int );

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name1 ) = lower(religiondictionary.name);

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name2) = lower(religiondictionary.name);

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name3) = lower(religiondictionary.name);

insert into Religion
select temp.*, '2' from temp;

___________________________________________________

drop table if exists temp;
create table temp (user_id string, name string );

insert into temp 
select 
AddressbookName.user_id, 
AddressbookName.name 
from AddressbookName
where AddressbookName.user_id 
not in 
(select religion.user_id from religion);

drop table if exists SplitedNames;
CREATE table SplitedNames(user_id string , name1 string, name2 string, name3 string );

insert into SplitedNames
select user_id, 
split( name, ' ')[0],
split( name, ' ')[1],
split( name, ' ')[2]
from temp;

drop table if exists temp;
create table temp(user_id string, name string, religion string, count int );

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name1) = lower(religiondictionary.name);

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name2) = lower(religiondictionary.name);

insert into temp 
select 
splitednames.user_id,
religiondictionary.name,
religiondictionary.religion,
religiondictionary.number
from splitednames inner join religiondictionary 
on
lower(splitednames.name3) = lower(religiondictionary.name);

insert into Religion
select temp.*, '3' from temp;

_______________________________________________________________________

drop table if exists SplitedNames1;
CREATE table SplitedNames1(user_id string , name1 string, name2 string, name3 string );

insert into SplitedNames1
select * from SplitedNames where user_id not in 
(select user_id from religion);

drop table if exists temp;
create table temp(user_id string, name string, religion string, count int );

insert into temp
select 
Splitednames1.user_id, 
SurnameDictionary.surname,
SurnameDictionary.religion.
'1' from Splitednames1 inner join SurnameDictionary 
on
lower(Splitednames1.name1) = lower(SurnameDictionary.surname);

insert into temp
select 
Splitednames1.user_id, 
SurnameDictionary.surname,
SurnameDictionary.religion.
'1' from Splitednames1 inner join SurnameDictionary 
on
lower(Splitednames1.name2) = lower(SurnameDictionary.surname);

insert into temp
select 
Splitednames1.user_id, 
SurnameDictionary.surname,
SurnameDictionary.religion.
'1' from Splitednames1 inner join SurnameDictionary 
on
lower(Splitednames1.name3) = lower(SurnameDictionary.surname);

_______________________________________________________________________________

















