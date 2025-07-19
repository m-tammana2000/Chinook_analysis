use chinook;
select * from album;
select * from artist;
select * from customer;
select * from employee;
select * from genre;
select * from invoice;
select * from invoice_line;
select * from media_type;
select * from playlist;
select * from playlist_track;
select * from track;
select * from album;

select * from customer;
create view customer_view  as
select customer_id,	first_name,	last_name,	coalesce(company, "NA") as company,
coalesce(address, "NA") as address, city, coalesce(state, "NA") as state ,
country, coalesce(postal_code,"NA") as postal_code, coalesce(phone,"NA")as phone,	
coalesce(fax,"NA") as fax,	email,	support_rep_id
from customer;

select * from customer_view;

create view track_view
as select track_id, name, album_id, media_type_id,	genre_id, coalesce(composer,"NA") as composer,	
milliseconds, bytes, unit_price
from track;

select * from track_view;

select t.track_id, t.name as track_name, t.genre_id, g.name as genre_name, sum(i.unit_price*i.quantity)  as total_price
from invoice_line i inner join track_view t on i.track_id = t.track_id inner join genre g on
g.genre_id = t.genre_id group by t.track_id, t.name, t.genre_id, g.name order by total_price;

select country, count(distinct customer_id) as no_of_customers
from customer_view
group by country
order by no_of_customers desc;

select * from customer_view;

select t.track_id, t.name as track_name, sum(il.quantity) as total_sold, g.name as genre_name, a.name as artist from
invoice_line il
inner join invoice i on il.invoice_id = i.invoice_id
inner join customer c on i.customer_id = c.customer_id
inner join track t on il.track_id = t.track_id
inner join album al on t.album_id = al.album_id
inner join artist a on al.artist_id = a.artist_id
inner join genre g on t.genre_id = g.genre_id
where c.country = 'USA'
group by t.track_id, t.name, g.name, a.name
order by total_sold desc limit 10;


select  a.artist_id, a.name as artist_name, sum(il.quantity) as total_quantities_sold from
invoice_line il
inner join invoice i on il.invoice_id = i.invoice_id
inner join customer c on i.customer_id = c.customer_id
inner join track t on il.track_id = t.track_id
inner join album al on t.album_id = al.album_id
inner join artist a on al.artist_id = a.artist_id
inner join genre g on t.genre_id = g.genre_id
where c.country = 'USA'
group by a.artist_id, a.name
order by total_quantities_sold desc  limit 1;

select  g.genre_id, a.artist_id, g.name as genre_name, sum(il.quantity) as total_quantities_sold from
invoice_line il
inner join invoice i on il.invoice_id = i.invoice_id
inner join customer c on i.customer_id = c.customer_id
inner join track t on il.track_id = t.track_id
inner join album al on t.album_id = al.album_id
inner join artist a on al.artist_id = a.artist_id
inner join genre g on t.genre_id = g.genre_id
where c.country = 'USA' and a.artist_id = 152
group by g.genre_id, a.artist_id, g.name
order by total_quantities_sold desc ;

select first_name, last_name, country, coalesce(state,"NA"), city from customer;

select  country, coalesce(state,"NA") as state, city, count(*) as total_customers from customer
group by country, coalesce(state,"NA"), city
order by  total_customers desc,country;


select * from invoice;

select  c.country, coalesce(c.state,"NA"), c.city, sum(i.total) as total_revenue, count(invoice_id) as total_invoices
 from customer c inner join invoice i 
 on i.customer_id = c.customer_id
group by c.country, coalesce(c.state,"NA"), c.city
order by country;


with cte as (select  c.customer_id, c.first_name, c.last_name,  c.country,  sum(i.total) as total_revenue, 
dense_rank() over(partition by c.country order by sum(i.total) desc) as rnk
 from customer_view c inner join invoice i 
 on i.customer_id = c.customer_id
group by c.customer_id, c.country
order by c.country, rnk)
select customer_id, concat(first_name," ", last_name) as customer_name, country, total_revenue, rnk as "rank" from cte where rnk between 1 and 5;

select c.customer_id, concat(first_name," " ,last_name) as Full_name, t.track_id, 
t.name as track_name, sum(il.quantity) as no_of_quantities
from customer c inner join invoice i on
c.customer_id = i.customer_id
inner join invoice_line il on 
i.invoice_id = il.invoice_id
inner join track t on 
t.track_id = il.track_id
group by c.customer_id, concat(first_name," " ,last_name), t.track_id, t.name
order by customer_id ,no_of_quantities;


select c.customer_id, concat(first_name, " ",last_name) as full_name, t.name as track_name, sum(quantity) as total_quantities from customer c
inner join invoice i on i.customer_id = c.customer_id
inner join invoice_line il on il.invoice_id = i.invoice_id
inner join track t on t.track_id = il.track_id
group by c.customer_id, concat(first_name, " ",last_name), t.name 
order by total_quantities desc;


select c.customer_id,concat(first_name, " ",last_name) as full_name, year(invoice_date)as "year", 
count(invoice_id) as total_orders
from customer_view c inner join invoice i
on i.customer_id=c.customer_id
group by c.customer_id, concat(first_name, " ",last_name),year(invoice_date)
order by customer_id, total_orders desc;

select c.customer_id,concat(first_name, " ",last_name) as full_name, year(invoice_date)as "year", 
avg(total) as total_avg_orders
from customer_view c inner join invoice i
on i.customer_id=c.customer_id
group by c.customer_id, concat(first_name, " ",last_name),year(invoice_date)
order by customer_id, total_avg_orders desc;

with cte as (select c.customer_id, invoice_date from customer_view c inner join  invoice i
on i.customer_id=c.customer_id),
churn as (select  year(invoice_date)as "year", count(customer_id)as no_of_customer, 
lag(count(customer_id)) over(order by year(invoice_date)) as prev_yr_count
from  cte group by year(invoice_date))
select year, no_of_customer, prev_yr_count, (( prev_yr_count/no_of_customer)*100) ,
case when no_of_customer> prev_yr_count then "decrease"
else "increase" end as growth_rate
 from churn;
 
 
 use chinook;
 with cte as (select c.customer_id, invoice_date from customer_view c inner join  invoice i
on i.customer_id=c.customer_id),
churn as (select  year(invoice_date) as "year", count(distinct customer_id)as no_of_customer, 
lag(count(distinct customer_id)) over(order by year(invoice_date)) as prev_yr_count
from  cte group by year(invoice_date))
select year, no_of_customer, prev_yr_count, ((no_of_customer-prev_yr_count)*100/prev_yr_count) as churn_rate,
case when no_of_customer> prev_yr_count then "increase"
else "decrease" end as growth_rate
 from churn;
 
with cte_usa as (select t.genre_id, g.name as genre_name,
 sum(i.quantity * i.unit_price) as total_genre_price, ic.billing_country as country
 from invoice_line i inner join 
 track t on t.track_id = i.track_id
 inner join invoice ic on ic.invoice_id = i.invoice_id
 inner join genre g on g.genre_id = t.genre_id
 where ic.billing_country = "USA" 
 group by t.genre_id, ic.billing_country,g.name
 order by total_genre_price desc),
cte_usa1 as ( select country, sum(total_genre_price) as total_usa_sales from cte_usa group by country)
select genre_id, genre_name, total_genre_price, total_usa_sales,
round((total_genre_price/ total_usa_sales)*100,2) as genre_sales_usa_percent
 from cte_usa cross join cte_usa1 
 order by genre_sales_usa_percent desc;


with cte_usa as (select t.genre_id, g.name as genre_name,
 sum(i.quantity * i.unit_price) as total_genre_price, ic.billing_country as country
 from invoice_line i inner join 
 track t on t.track_id = i.track_id
 inner join invoice ic on ic.invoice_id = i.invoice_id
 inner join genre g on g.genre_id = t.genre_id
 group by t.genre_id, ic.billing_country,g.name
 order by total_genre_price desc),
cte2 as (select genre_id, genre_name, total_genre_price, country, dense_rank () over(partition by country order by total_genre_price desc) as rnk
 from cte_usa 
 order by total_genre_price desc)
 select genre_id, genre_name, total_genre_price, country, rnk from cte2 where rnk in(1,2);


with cte_usa as (select t.genre_id, g.name as genre_name,
 sum(i.quantity * i.unit_price) as total_genre_price
 from invoice_line i inner join 
 track t on t.track_id = i.track_id
 inner join invoice ic on ic.invoice_id = i.invoice_id
 inner join genre g on g.genre_id = t.genre_id
 group by t.genre_id, g.name
 order by total_genre_price desc)
 select genre_id, genre_name, total_genre_price 
 from cte_usa 
 order by total_genre_price desc;
  
 select distinct t.album_id, a.artist_id,  ar.name as artist_name,  t.genre_id from track t 
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id where t.genre_id in (1,4,3)
 order by artist_i;
 
with cte as ( select c.customer_id, concat(first_name, " ",last_name ) as full_name,
 t.track_id, g.genre_id  from customer c 
 inner join invoice i on c.customer_id = i.customer_id
 inner join invoice_line il on il.invoice_id = i.invoice_id
 inner join track t on t.track_id = il.track_id
 inner join genre g on g.genre_id = t.genre_id )
 select customer_id, full_name,  count(distinct genre_id) as no_of_genre from cte
 group by customer_id, full_name
 having count(distinct genre_id) >=3;

 with cte as (select g.genre_id, g.name as genre_name, sum(il.unit_price * il.quantity) as total_price, i.billing_country as country from invoice_line il
 inner join track t on t.track_id = il.track_id
 inner join genre g on g.genre_id=t.genre_id
 inner join invoice i on i.invoice_id = il.invoice_id
 where i.billing_country= "USA"
 group by t.track_id, g.genre_id, i.billing_country),
cte1 as (select genre_id, genre_name, country, sum(total_price) as total_amount, 
 dense_rank() over(order by sum(total_price) desc) as rank_of_genre
 from cte
 group by genre_id)
 select genre_id, genre_name, total_amount, rank_of_genre, country from cte1;
 
 use chinook;

 
with last_date as
( select  max(invoice_date) as max_invoice_date, customer_id from invoice group by customer_id),
customers_purchases as (select distinct c.customer_id, concat(first_name, " ", last_name) as full_name from customer c
 left join last_date l on  c.customer_id = l.customer_id
 left join invoice i on  c.customer_id = i.customer_id 
 where i.invoice_date < date_sub(l.max_invoice_date, interval 3 month))
 select customer_id, full_name from customers_purchases
 order by customer_id;
 
 select customer_id, invoice_date  from invoice order by invoice_date desc;
 
 
with cte as (select g.genre_id, ar.name as artist_name, g.name as genre_name, t.album_id, 
 a.title as album_name, i.unit_price, i.quantity, sum(i.unit_price*i.quantity) as total_sales, invoice.billing_country from track t 
 inner join invoice_line i on t.track_id =i.track_id
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id
 inner join genre g on g.genre_id = t.genre_id
 inner join invoice on invoice.invoice_id = i.invoice_id
where invoice.billing_country = "USA"
 group by t.album_id,ar.name, g.name, g.genre_id,i.unit_price, i.quantity
 order by total_sales desc, album_name)
select genre_id, genre_name, album_id, album_name, artist_name, quantity, unit_price, total_sales,
dense_rank() over (order by total_sales desc) as ranking
 from cte;
 

  with cte as (select g.genre_id, g.name as genre_name, sum(i.unit_price*i.quantity) as total_sales from track t 
 inner join invoice_line i on t.track_id =i.track_id
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id
 inner join genre g on g.genre_id = t.genre_id
 inner join invoice on invoice.invoice_id = i.invoice_id
where invoice.billing_country <> "USA"
 group by  g.name, g.genre_id
 order by total_sales desc)
select genre_id, genre_name, total_sales,
dense_rank() over (order by total_sales desc) as ranking
 from cte;
 
 
   with cte as (select g.genre_id, g.name as genre_name, sum(i.quantity) as total_quantity, invoice.billing_country as country from track t 
 inner join invoice_line i on t.track_id =i.track_id
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id
 inner join genre g on g.genre_id = t.genre_id
 inner join invoice on invoice.invoice_id = i.invoice_id
where invoice.billing_country <> "USA"
 group by  g.name, g.genre_id, invoice.billing_country 
 order by total_quantity desc)
select genre_id, genre_name, country, total_quantity
 from cte;

 
    with cte as (select g.genre_id, g.name as genre_name, sum(i.quantity) as total_quantity, invoice.billing_country as country from track t 
 inner join invoice_line i on t.track_id =i.track_id
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id
 inner join genre g on g.genre_id = t.genre_id
 inner join invoice on invoice.invoice_id = i.invoice_id
 group by  g.name, g.genre_id, invoice.billing_country 
 order by total_quantity desc)
select genre_id, genre_name, country, total_quantity
 from cte;
 
    with cte as (select g.genre_id, g.name as genre_name, sum(i.quantity) as total_quantity from track t 
 inner join invoice_line i on t.track_id =i.track_id
 inner join album a on a.album_id = t.album_id
 inner join artist ar on ar.artist_id = a.artist_id
 inner join genre g on g.genre_id = t.genre_id
 inner join invoice on invoice.invoice_id = i.invoice_id
where invoice.billing_country <> "USA"
 group by  g.name, g.genre_id
 order by total_quantity desc)
select genre_id, genre_name,  total_quantity
 from cte;
 
 
 with customerinvoicedates as (
	select 
		c.customer_id,c.first_name, c.last_name, 
        min(date(i.invoice_date)) as first_purchase_date,
        max(date(i.invoice_date)) as last_purchase_date,
        count(distinct i.invoice_id) as purchase_frequency,
        round(avg(il.quantity),2) as avg_basket_size,
        round(avg(i.total),2) as avg_spending_amount
    from customer c
    join invoice i on c.customer_id = i.customer_id
    join invoice_line il on i.invoice_id = il.invoice_id
    group by c.customer_id,c.first_name, c.last_name),
    customercategory as (
	select 
		*,
        datediff(last_purchase_date,first_purchase_date) as date_diff,
        case when datediff(last_purchase_date,first_purchase_date) > 910 then 'long term' else 'new' end as category_type
	from customerinvoicedates)
select * from customercategory order by customer_id;


select a1.title as album_1, a2.title as album_2, count(invoice_id) from album a1 
inner join album a2 on a1.album_id > a2.album_id
inner join track t on t.album_id = a1.album_id 
inner join invoice_line i on i.track_id = t.track_id
group by a1.title, a2.title;


with invoice_album as (
select distinct i.invoice_id, t.album_id, g.name as genre_name from invoice_line il
inner join track t on il.track_id = t.track_id
inner join genre g on t.genre_id = g.genre_id
inner join invoice i on il.invoice_id = i.invoice_id)
select ia1.genre_name, a1.title as album_1, a2.title as album_2,
count(distinct ia1.invoice_id) as invoice_count
from invoice_album ia1
inner join invoice_album ia2 on ia1.invoice_id = ia2.invoice_id and ia1.album_id > ia2.album_id
inner join album a1 on ia1.album_id = a1.album_id
inner join album a2 on ia2.album_id = a2.album_id
group by a1.title, a2.title, ia1.genre_name
order by invoice_count desc;


with cte as (select billing_country as country, billing_state as state, billing_city as city, count(distinct customer_id) as total_customers,
 count(distinct i.invoice_id) as total_purchases,
 sum(quantity* il.unit_price)as total_quantity_sales
 from invoice i
 inner join invoice_line il on i.invoice_id = il.invoice_id
 group by billing_country, billing_state, billing_city
 order by total_quantity_sales desc)
 select *, round(total_quantity_sales/total_purchases, 2) as avg_salesby_purchases,
 round(total_purchases/total_customers,2) as avg_price_by_customer from cte;
 
 
with cte as ( select c.customer_id, i.billing_country as country, min(invoice_date) as first_date,
 max(invoice_date) as last_date, count(i.invoice_id) as purchases
 from customer c left join invoice i
 on c.customer_id = i.customer_id
 group by c.customer_id, i.billing_country
 order by purchases desc)
 select country, date(first_date), date(last_date), purchases from cte;
 
 select  i.billing_country as country, count(distinct c.customer_id) as total_customers, min(invoice_date) as first_date,
 max(invoice_date) as last_date, count(i.invoice_id) as purchases,
 case when count(distinct c.customer_id)>=5 then "Premium Region"
 else "Normal Region" end as Region_category
 from customer c left join invoice i
 on c.customer_id = i.customer_id
 group by i.billing_country
 order by purchases desc;
 
 
 
 select  i.billing_country as country, i.billing_state as state, i.billing_city as city, count(distinct c.customer_id) as total_customers,
 count( distinct i.invoice_id) as purchase_orders, round(count(distinct i.invoice_id)/count(distinct c.customer_id),2) as avg_cust_orders,
 round(avg(i.total),2) as avg_spending_amount,
 case when round(count(distinct i.invoice_id)/count(distinct c.customer_id),2) >10 then "Low Risk"
 else "High Risk" end as Risk_category
 from customer c left join invoice i
 on c.customer_id = i.customer_id
 inner join invoice_line il on il.invoice_id = i.invoice_id
 group by i.billing_country , i.billing_state , i.billing_city 
 order by purchase_orders desc;
 
 
with cte as  (select c.customer_id, concat(first_name, " ", last_name) as full_name, billing_country, billing_city,
 datediff( max(i.invoice_date), min(i.invoice_date)) as customer_tenure, count(i.invoice_date) as total_purchases,
 sum(i.total) as total_purchase, avg(i.total) as avg_purchase
 from customer c left join invoice i on
 c.customer_id = i.customer_id
 group by c.customer_id, concat(first_name, " ", last_name), billing_country, billing_city 
 )
 select *, case when customer_tenure>1000 then "Long Term"
 else "Short Term" end as Tenure_category, total_purchase as life_time_value  from cte 
order by total_purchases desc, customer_tenure desc;


alter table album
modify column ReleaseYear int;

alter table album
add column ReleaseYear tinyint;

alter table album
modify column ReleaseYear int;

select * from album;

update album set ReleaseYear = 2017
 where album_id in (1,2,3,4,5);

update album set ReleaseYear = 2018
 where album_id in (6,7,8,9,10); 


with tracks_by_customer as (
    select i.customer_id, sum(il.quantity) as total_tracks
    from invoice i
    inner join invoice_line il on i.invoice_id = il.invoice_id
    group by i.customer_id
),
customer_spent as (
    select c.country, c.customer_id, sum(i.total) as total_spent, tpc.total_tracks
    from customer c
    inner join invoice i on c.customer_id = i.customer_id
    inner join tracks_by_customer tpc on c.customer_id = tpc.customer_id
    group by c.country, c.customer_id, tpc.total_tracks
)
select cs.country,
count(distinct cs.customer_id) as number_of_customers,
round(avg(cs.total_spent),2) as average_amount_spent_by_customer,
round(avg(cs.total_tracks),2) as average_tracks_purchased_by_customer
from customer_spent cs
group by cs.country
order by average_amount_spent_by_customer desc;


select c.country, round(sum(total),2) as total from invoice i inner join customer_view c
on c.customer_id = i.customer_id
group by c.country
order by total desc;

 