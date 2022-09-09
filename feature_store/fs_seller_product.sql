with tb_join_all as (
  select t2.idSeller,
         t3.*,
         t1.*
         

  from silver_olist.orders as t1

  inner join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.products as t3
  on t2.idProduct = t3.idProduct

  where t1.dtPurchase < '{date}'
  and t1.dtPurchase >= add_months('{date}',-6)
)

select idSeller,
       avg(vlWeightGramas) as vlAvgWeight,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then vlWeightGramas end),0) as vlAvgWeight1M,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then vlWeightGramas end),0) as vlAvgWeight3M,
       
       avg(nrNameLength) as nrAvgNameLength,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then nrNameLength end),0) as nrAvgNameLength1M,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then nrNameLength end),0)as nrAvgNameLength3M,
       
       avg(nrPhotos) as vlAvgPhotos,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then nrPhotos end),0) as vlAvgPhotos1M,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then nrPhotos end),0) as vlAvgPhotos3M,
       
       avg(vlLengthCm * vlHeightCm * vlWidthCm) as vlAvgProductVolume,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then vlLengthCm * vlHeightCm * vlWidthCm end),0) as vlAvgProductVolume1M,
       coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then vlLengthCm * vlHeightCm * vlWidthCm end),0) as vlAvgProductVolume3M,
       
       count(distinct idProduct) as qtProducts,
       count(distinct case when datediff('{date}', dtPurchase) < 30 then idProduct end ) as qtProducts1M,
       count(distinct case when datediff('{date}', dtPurchase) < 90 then idProduct end ) as qtProducts3M,
       
       count(distinct descCategoryName) as qtCategoryType,
       count(distinct case when datediff('{date}', dtPurchase) < 30 then descCategoryName end ) as qtCategoryType1M,
       count(distinct case when datediff('{date}', dtPurchase) < 90 then descCategoryName end ) as qtCategoryTyp3M       
       

from tb_join_all

group by idSeller