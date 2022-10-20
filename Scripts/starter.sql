USE flights;

show tables;

/* Primary Key On time Reporting */
select day_of_month,month,year,TAIL_NUM,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID,count(*) total from ontime_reporting
 group by day_of_month,month,year,TAIL_NUM,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID
 order by total desc;
 
/*Primary key for carrier_decode*/
select a.airline_id,a.op_unique_carrier, carrier_name, count(*) total from 
(select distinct airline_id,op_unique_carrier,carrier_name 
from carrier_decode) a
group by a.airline_id,a.op_unique_carrier,a.carrier_name
order by total desc;


/*Primary key for P10_EMPLOYEES*/
select a.year,a.airline_id,a.op_unique_carrier, carrier_name, entity, count(*) total from 
p10_employees a
group by a.year,a.airline_id,a.op_unique_carrier,a.carrier_name, entity
order by total desc;
