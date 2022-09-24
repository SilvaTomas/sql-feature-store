with tb_join_all as (

  SELECT t1.idOrder,
         t1.dtPurchase,
         t1.dtDeliveredCustomer,
         t2.idSeller,
         t3.idReview,
         t3.vlscore,
         t3.dtCreation,
         t3.dtAnswer,
         t3.descMessage


    FROM silver_olist.orders AS t1

    LEFT JOIN silver_olist.order_items AS t2
    ON t1.idOrder = t2.idOrder

    LEFT JOIN silver_olist.order_review AS t3
    ON t1.idOrder = t3.idOrder


    WHERE t1.dtPurchase < '{date}'
    AND t1.dtPurchase >= add_months('{date}', -6)
    AND t2.idSeller is not null

),

tb_seller_order_review as (


       SELECT idOrder, idSeller, dtPurchase, dtDeliveredCustomer,
              count(distinct idReview) as qtReviews,
              avg(vlscore) as avgScoreReview,
              min(dtCreation) as minDtReview,
              max(dtCreation) as maxDtReview,
              min(dtAnswer) as minDtAnswer,
              max(dtAnswer) as maxDtAnswer,
              sum(case when descMessage is not null then 1 else 0 end) as qtMensagem
       
       FROM tb_join_all
       GROUP BY 1, 2, 3, 4

),

tb_summary as(
  SELECT idSeller, 
         avg(case when avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo,
         avg(case when avgScoreReview >= 3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro,
         avg(case when avgScoreReview >= 4 then 1 else 0 end) as pctScorePositivo,
         avg(case when minDtAnswer is not null then 1 else 0 end) as pct_resposta,
         sum(qtReviews) as qtReviews,
         sum(qtMensagem) as qtMensagem,
         sum(qtMensagem) / sum(qtReviews) as pctMensagem,
         avg(datediff(minDtAnswer, minDtReview)) as avgTempoResposta,
         avg(datediff(minDtReview, dtDeliveredCustomer)) as avgTempoReview,

         sum(case when datediff('{date}',dtPurchase)< 30 then qtReviews end) as qtReviews1M,
         sum(case when datediff('{date}',dtPurchase)< 30 then qtMensagem end) as qtMensagem1M,
         avg(case when datediff('{date}',dtPurchase)< 30 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta1M,
         avg(case when datediff('{date}',dtPurchase)< 30 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview1M,

         sum(case when datediff('{date}',dtPurchase)< 90 then qtReviews end) as qtReviews3M,
         sum(case when datediff('{date}',dtPurchase)< 90 then qtMensagem end) as qtMensagem3M,
         avg(case when datediff('{date}',dtPurchase)< 90 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta3M,
         avg(case when datediff('{date}',dtPurchase)< 90 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview3M
  FROM tb_seller_order_review
  GROUP BY idSeller)


SELECT idSeller,
       pctScoreNegativo,
       pctScoreNeutro,
       pctScorePositivo,
       pct_resposta,
       qtReviews,
       qtMensagem,
       pctMensagem,
       avgTempoResposta,
       avgTempoReview,
       qtReviews1M,
       qtMensagem1M,
       qtMensagem1M / qtReviews1M as pctMensagem1M,
       avgTempoResposta1M,
       avgTempoReview1M,
       qtReviews3M,
       qtMensagem3M,
       qtMensagem3M / qtReviews3M as pctMensagem3M,
       avgTempoResposta3M,
       avgTempoReview3M

FROM tb_summary