create table estudiantesPR (region_stud STRING, district_stud STRING, id_school_stud INT, school_name_stud STRING, lvl_school_stud STRING, sex_stud CHAR(1), id_student INT) row format delimited
fields terminated by ',';

create table schoolsPR (region_school STRING, district_school STRING, city STRING, id_school INT, school_name STRING, lvl_school STRING, college INT) row format delimited
fields terminated by ',';

load data local inpath "/home/osboxes/exam1-sp17-bigdata-desc/hive/studentsPR.csv" into table studentsPR;

load data local inpath "/home/osboxes/exam1-sp17-bigdata-desc/hive/escuelasPR.csv" into table schoolsPR;

Create table joinedStudentsSchools as
select *
from studentspr as S, schoolspr as E
Where  S.id_school_stud = E.id_school;

select region_stud,city,count(*) 
from joinedStudentsSchools 
group by region_stud,city;

select lvl_school, city, count(*) 
from schoolsPR 
group by city, lvl_school;

select count(*) 
from joinedStudentsSchools
where sex_stud = 'F' and city = 'Ponce' and lvl_school = 'Superior';

select region_stud,district_school,city,count(*) 
from joinedStudentsSchools 
where sex_stud =  'M'
group by region_stud,district_school,city;

