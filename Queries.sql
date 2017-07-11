###########################  Creating Final Names ##################################

drop table if exists temp;
create table temp (user_id string, msisdn bigint );

insert into temp
select 
id, msisdn from 
hike.users where id in 
(select user_id  from hike.user_activations where type = "new" and dt = "2017-07-11" )

drop table if exists AddressbookName;
create table AddressbookName (user_id string, name string );
insert into AddressbookName
select 
temp.user_id, temp1.name 
from temp inner join 
(select contact_msisdn as msisdn , contact_name as name from hike.addressbook_main_snapshot 
where 
contact_msisdn in (select msisdn from temp)
)temp1 
on
temp.msisdn = temp1.msisdn;

drop table if exists finalnames;
create table finalnames(user_id string, name string, count string);

insert into finalnames 
select
user_id, name, num from
(select user_id as user_id,  name as name ,  number as num ,rank() over (order by number desc) as r FROM 
 (select AddressbookName.user_id as user_id , AddressbookName.name as name , count(AddressbookName.user_id)      as number from AddressbookName group by AddressbookName.user_id, AddressbookName.name ) B
) A
WHERE A.r = 1;


################################ Exact Match Search  #################################################



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


################################  Match After Splitting #################################################


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


###########################  Match From All Addressbook Names  #####################################

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


###########################  Surname Search  #####################################


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

insert into Religion
select temp.*, '4' from temp;



#################################################################################################









