with tb_join_all AS (

  SELECT t2.*,
         t3.idSeller

  FROM silver_olist.orders as t1

  left join silver_olist.order_payment as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.order_items as t3
  on t1.idOrder = t3.idOrder

  WHERE t1.dtPurchase < '{date}'
  AND t1.dtPurchase >= add_months('{date}',-6)
)

SELECT '{date}' as dtReference,
       idseller, 
       count(distinct descType) as qtPaymentType,
       avg(nrInstallments) as nrAvgInstallments,
       max(nrInstallments) as nrMaxInstallments,
       min(nrInstallments) as nrMinInstallments,
       
       sum(case when descType = 'boleto' then 1 else 0 end) / count(distinct idOrder) as pctBoletoCount,
       sum(case when descType = 'not_defined' then 1 else 0 end) / count(distinct idOrder) as pctNot_definedCount,
       sum(case when descType = 'credit_card' then 1 else 0 end) / count(distinct idOrder) as pctCredit_cardCount,
       sum(case when descType = 'voucher' then 1 else 0 end) / count(distinct idOrder) as pctVoucherCount,
       sum(case when descType = 'debit_card' then 1 else 0 end) / count(distinct idOrder) as pctDebit_cardCount,
       
       sum(case when descType = 'boleto' then vlPayment else 0 end) / sum(vlPayment) as pctBoletoRevenue,
       sum(case when descType = 'not_defined' then vlPayment else 0 end) / sum(vlPayment) as pctNot_definedRevenue,
       sum(case when descType = 'credit_card' then vlPayment else 0 end) / sum(vlPayment) as pctCredit_cardRevenue,
       sum(case when descType = 'voucher' then vlPayment else 0 end) / sum(vlPayment) as pctVoucherRevenue,
       sum(case when descType = 'debit_card' then vlPayment else 0 end) / sum(vlPayment) as pctDebit_cardRevenue
     
FROM tb_join_all

GROUP BY idSeller