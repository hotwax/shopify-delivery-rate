<@compress single_line=true>
    <#macro buildSqlCondition value>
        ${Static["co.hotwax.order.routing.OrderRoutingHelper"].makeSqlWhere(value)!}
    </#macro>
    <#assign invenoryGroupFiter = inventoryFilterMap.get("facilityGroupId")! />
    <#assign brokeringSafetyStock = inventoryFilterMap.get("brokeringSafetyStock")! />
    <#assign distance = inventoryFilterMap.get("distance")! />
    <#assign ignoreFacilityOrderLimitCond = inventoryFilterMap.get("ignoreFacilityOrderLimit")! />
    <#assign ignoreFacilityOrderLimit = Static["org.moqui.util.ObjectUtilities"].basicConvert((ignoreFacilityOrderLimitCond.fieldValue)!'false', 'Boolean') />
  select
  y.ORDER_ID,
  y.ORDER_ITEM_SEQ_ID,
  y.PRODUCT_ID,
  y.ship_group_total_qty,
  y.ITEM_QTY,
  y.ROUTED_ITEM_QTY,
  y.FACILITY_TYPE_ID,
  y.FACILITY_ID,
  y.ORIGIN_POSTAL_CODE,
  y.distance,
  y.rank_by_order_at_facility as RANK_BY_ORDER_AT_FACILITY,
  y.rank_by_item_cnt AS RANK_BY_ITEM_CNT,
  y.FACILITY_EXHAUSTED,
  y.total_inv as LAST_INVENTORY_COUNT,
  y.inventoryForAllocation as INVENTORY_FOR_ALLOCATION,
  ifnull(y.ALLOW_BROKERING, 'Y') as ALLOW_BROKERING,
  y.MAXIMUM_ORDER_LIMIT,
  y.LAST_ORDER_COUNT
  from (select
  @rn := @rn+1 as row_num,
  case when locate(concat(x.facility_id,"-",x.product_id),@fpuim) = 0 then x.total_inv else substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1) end as unalloc_inv,ifnull(x.last_order_count,0) +1 as u,
  @rtd :=case
  when (locate(concat(x.facility_id,"-",x.product_id),@fpuim)= 0 <#if !ignoreFacilityOrderLimit>and (((ifnull(x.last_order_count,0) +1 < x.maximum_order_limit) OR x.maximum_order_limit is null))</#if>) or (locate(concat(x.facility_id,"-",x.product_id),@fpuim) != 0 and substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1) > 0) then
  (case when @oh !=x.ORDER_ID then (case when x.total_inv < x.item_qty then round(x.total_inv,0) else round(x.item_qty,0) end )
  when @oh=x.order_id and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps)=0 and locate(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@ps)= 0 and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s)=0 then (case when x.total_inv >= x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) then x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) else x.total_inv end)
  when @oh=x.order_id and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps) !=0 and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s)=0 then (case when x.total_inv >= x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) then x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) else x.total_inv end)
  when find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps)=0 and locate(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@ps) != 0 then (case when x.total_inv >= x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) then x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) else x.total_inv end)
  else 0 end)
  else 0 end
  as routed_item_qty,
  @aq := case
  when @oh != x.order_id and @rtd = 0 then 0
  when @oh != x.order_id and @rtd > 0 then @rtd
  when @oh = x.order_id and @rtd > 0 then @aq+@rtd
  else @aq end
  as allocated_ord_qty,
  @r :=case when @rtd > 0 then 1 else 0 end as retained,
  @fom := case
  when find_in_set(concat(x.facility_id,"-",x.order_id),@fom)= 0 then (case when @rtd =0 then @fom when @rtd > 0 then concat(@fom,",",x.facility_id,"-",x.order_id) end)
  when find_in_set(concat(x.facility_id,"-",x.order_id),@fom) != 0 then @fom end
  as facility_order_map,
  ifnull(x.last_order_count,0)+round((char_length(@fom) - char_length(REPLACE(@fom,x.FACILITY_ID,'')))/char_length(x.FACILITY_ID)) as allocated_ord_cnt,
    <#if !ignoreFacilityOrderLimit>case when ((ifnull(x.last_order_count,0) +1 < x.maximum_order_limit) OR x.maximum_order_limit is null) then 'N' else 'Y' end <#else>'N'</#if> as facility_exhausted,
  @ps := case
  when (locate(concat(x.facility_id,"-",x.product_id),@fpuim)= 0 <#if !ignoreFacilityOrderLimit>and (((ifnull(x.last_order_count,0) +1 < x.maximum_order_limit) OR x.maximum_order_limit is null))</#if>) or (locate(concat(x.facility_id,"-",x.product_id),@fpuim) != 0 and substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1) > 0) then
  (case
  when @oh !=x.ORDER_ID then (case when x.total_inv < x.item_qty then concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.total_inv,0)) else 0 end)
  when @oh=x.order_id and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s) !=0 then @ps
  when @oh=x.order_id and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps)=0 and locate(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@ps)=0 and x.total_inv < x.item_qty then concat(@ps,",",x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.total_inv,0))
  when @oh=x.order_id and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps)=0 and locate(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@ps) != 0 then replace(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1)),concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1)+(case when x.total_inv >= x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) then x.item_qty - substring_index(substring_index(@ps,concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":"),-1),",",1) else x.total_inv end)))
  else @ps end)
  else @ps end
  as partial_alloc_item,
  @s := case
  when (locate(concat(x.facility_id,"-",x.product_id),@fpuim)= 0 <#if !ignoreFacilityOrderLimit>and (((ifnull(x.last_order_count,0) +1 < x.maximum_order_limit) OR x.maximum_order_limit is null))</#if>) or (locate(concat(x.facility_id,"-",x.product_id),@fpuim) != 0 and substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1) > 0) then
  (case
  when @oh != x.order_id and x.total_inv < x.item_qty then 0
  when @oh != x.order_id and x.total_inv >= x.item_qty then concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID)
  when @oh=x.ORDER_ID and x.total_inv >= x.item_qty and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s)=0 then concat(@s,",",x.order_id,"-",x.ORDER_ITEM_SEQ_ID)
  when @oh=0 and x.total_inv >= x.item_qty and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s)=0 then concat(@s,",",x.order_id,"-",x.ORDER_ITEM_SEQ_ID)
  when find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)),@ps) != 0 and find_in_set(concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID),@s)=0 then concat(@s,",",x.order_id,"-",x.ORDER_ITEM_SEQ_ID)
  else @s end)
  else @s end
  as allocated_item,
  @fpuim := case when locate(concat(x.facility_id,"-",x.product_id),@fpuim)= 0 then (case when @rtd =0 then @fpuim when @rtd > 0 then concat(@fpuim,",",x.facility_id,"-",x.product_id,":",x.total_inv-@rtd) end)
  when locate(concat(x.facility_id,"-",x.product_id),@fpuim) != 0 then (case when @rtd =0 then @fpuim when @rtd > 0 then replace(@fpuim,concat(x.facility_id,"-",x.product_id,":",substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1)),concat(x.facility_id,"-",x.product_id,":",substring_index(substring_index(@fpuim,concat(x.facility_id,"-",x.product_id,":"),-1),",",1)-@rtd))end) else @fpuim end as facility_prod_unalloc_inv_map,
  concat(x.order_id,"-",x.ORDER_ITEM_SEQ_ID,":",round(x.item_qty,0)) as item,
  @oh :=x.order_id,@oi :=x.ORDER_ITEM_SEQ_ID,@f :=x.FACILITY_ID,
  case when x.order_id is null and x.ORDER_ITEM_SEQ_ID is null then 'Y' else 'N' end as backordered,
  x.*
  from
  (select oisd.order_id,oisd.ORDER_ITEM_SEQ_ID,oisd.product_id,<#if invenoryGroupFiter?has_content>fgm.SEQUENCE_NUM as facilitySequence,fgm.FACILITY_GROUP_ID,</#if>f.facility_type_id,
  ifnull(ST_Distance_Sphere(point(fpa.LONGITUDE, fpa.LATITUDE),point('${longitude}','${latitude}')), 0) * ${conversionFactor} as distance,
  pf.last_inventory_count as ATP,pf.minimum_stock, (ifnull(pf.last_inventory_count,0)-ifnull(pf.MINIMUM_STOCK,0)) as total_inv,round(ifnull(pf.last_inventory_count, 0)/(oisd.quantity)*100,2) as availablity_pct,pf.facility_id,pf.ALLOW_BROKERING,
  '${cartItemQuantity}' as ship_group_total_qty,
  oisd.quantity as item_qty,
  fpa.postal_code as origin_postal_code,f.maximum_order_limit,foc.entry_date,foc.last_order_count, inv_count.inventoryForAllocation,
  ifnull((select distinct 'N' from ${customOrderItem} oi1 LEFT join product_facility pf1 on oi1.product_id=pf1.product_id and pf1.facility_id=pf.FACILITY_ID where oi1.order_id=oisd.ORDER_ID and ((ifnull(pf1.last_inventory_count,0)-ifnull(pf1.MINIMUM_STOCK,0)) < oi1.quantity or pf1.FACILITY_ID is null) AND ifnull(pf.ALLOW_BROKERING,'Y') = 'Y' group by oi1.ORDER_ID,pf1.FACILITY_ID),'Y') as rank_by_order_at_facility,<#-- revised -->
  ifnull((select distinct 'N' from ${customOrderItem} oi1 LEFT join product_facility pf1 on oi1.product_id=pf1.product_id and pf1.facility_id=pf.FACILITY_ID where oi1.order_id=oisd.ORDER_ID <#if brokeringSafetyStock?has_content> and ((ifnull(pf1.last_inventory_count,0)-ifnull(pf1.MINIMUM_STOCK,0)) < ${(brokeringSafetyStock.fieldValue)!0} OR pf1.FACILITY_ID is null) </#if> AND ifnull(pf.ALLOW_BROKERING,'Y') = 'Y' group by oi1.ORDER_ID,pf1.FACILITY_ID),'Y') as rank_by_order_above_facility_threshold,  <#-- revised -->
  ifnull((select count(oi2.order_item_seq_id) from ${customOrderItem} oi2 inner join product_facility pf2 on oi2.product_id=pf2.product_id and (ifnull(pf2.LAST_INVENTORY_COUNT,0)-ifnull(pf2.MINIMUM_STOCK,0)) > 0 and ifnull(pf2.ALLOW_BROKERING,'Y') = 'Y' where oi2.order_id=oisd.ORDER_ID and pf2.facility_id=pf.facility_id and (ifnull(pf2.last_inventory_count,0)-ifnull(pf2.MINIMUM_STOCK,0)) >= oi2.quantity /*and oi2.STATUS_ID = 'ITEM_APPROVED'*/ group by oi2.ORDER_ID,pf2.FACILITY_ID),0) as rank_by_item_cnt,
  ifnull((select count(oi2.order_item_seq_id) from ${customOrderItem} oi2 inner join product_facility pf2 on oi2.product_id=pf2.product_id <#if brokeringSafetyStock?has_content> and (ifnull(pf2.LAST_INVENTORY_COUNT,0)-ifnull(pf2.MINIMUM_STOCK,0)) <@buildSqlCondition value=brokeringSafetyStock /> </#if> and ifnull(pf2.ALLOW_BROKERING,'Y') = 'Y' where oi2.order_id=oisd.ORDER_ID and pf2.facility_id=pf.facility_id and (ifnull(pf2.last_inventory_count,0)-ifnull(pf2.MINIMUM_STOCK,0)) >= oi2.quantity /*and oi2.STATUS_ID = 'ITEM_APPROVED'*/ group by oi2.ORDER_ID,pf2.FACILITY_ID),0) as rank_by_item_cnt_above_threshold,
  ifnull((select 'Y' from ${customOrderItem} oi3 where oi3.order_id=oisd.ORDER_ID and oi3.order_item_seq_id=oisd.order_item_seq_id <#if brokeringSafetyStock?has_content> and (ifnull(pf.last_inventory_count,0)-ifnull(pf.MINIMUM_STOCK,0)) <@buildSqlCondition value=brokeringSafetyStock /> </#if> and (ifnull(pf.last_inventory_count,0)-ifnull(pf.MINIMUM_STOCK,0)) >= oi3.quantity /*and oi3.STATUS_ID = 'ITEM_APPROVED'*/),'N') as item_at_facility_above_threshold
  from ${customOrderItem!} oisd
  left join product_facility pf on oisd.product_id=pf.product_id and (ifnull(pf.LAST_INVENTORY_COUNT,0)-ifnull(pf.MINIMUM_STOCK,0)) > 0 and ifnull(pf.ALLOW_BROKERING,'Y') = 'Y'
  left join facility f on pf.facility_id=f.facility_id
  left join (select foc1.facility_id,foc1.entry_date,<#if !ignoreFacilityOrderLimit>foc1.last_order_count<#else> NULL as last_order_count</#if> from facility_order_count foc1
  inner join (select facility_id,max(entry_date) as entry_date from facility_order_count group by facility_id) foc2 on foc2.facility_id=foc1.facility_id and foc2.entry_date=foc1.entry_date
  ) foc on pf.facility_id=foc.facility_id and foc.entry_date = DATE(CONVERT_TZ(UTC_TIMESTAMP,'+00:00' , '${brokeringOffset!"+00:00"}'))
  left join facility_contact_mech_purpose fcmp on fcmp.facility_id=f.facility_id and fcmp.contact_mech_purpose_type_id='PRIMARY_LOCATION' and (fcmp.thru_date is null or fcmp.thru_date >= now())
  inner join postal_address fpa on fpa.contact_mech_id=fcmp.contact_mech_id
  left join (SELECT PFI.FACILITY_ID, sum(ifnull(PFI.LAST_INVENTORY_COUNT, 0)) AS inventoryForAllocation, count(PFI.PRODUCT_ID) as PRODUCT_COUNT FROM PRODUCT_FACILITY PFI WHERE ifnull(PFI.LAST_INVENTORY_COUNT, 0) > ifnull(PFI.MINIMUM_STOCK,0)
  and ifnull(PFI.ALLOW_BROKERING,'Y') = 'Y' AND PFI.PRODUCT_ID in (SELECT OII.PRODUCT_ID FROM ${customOrderItem} OII WHERE OII.ORDER_ID='${orderId}') group by PFI.FACILITY_ID having inventoryForAllocation > 0 order by PRODUCT_COUNT DESC, inventoryForAllocation DESC) inv_count on pf.facility_id = inv_count.facility_id
  inner join product_store_facility psf on oisd.product_store_id = psf.product_store_id and psf.facility_id = f.facility_id
    <#if invenoryGroupFiter?has_content>inner join (select fg.FACILITY_GROUP_TYPE_ID,fgrm.FACILITY_ID, fgrm.FACILITY_GROUP_ID,fgrm.SEQUENCE_NUM from facility_group_member fgrm inner join facility_group fg on fgrm.FACILITY_GROUP_ID=fg.FACILITY_GROUP_ID and fg.FACILITY_GROUP_TYPE_ID='BROKERING_GROUP' and (fgrm.THRU_DATE > now() or fgrm.THRU_DATE is null)) fgm on f.FACILITY_ID=fgm.FACILITY_ID</#if>
  where oisd.ORDER_ID='${orderId}'
  AND ((ifnull(foc.last_order_count,0) +1 < f.maximum_order_limit) OR f.maximum_order_limit is null)
    <#if invenoryGroupFiter?has_content>AND fgm.FACILITY_GROUP_ID <@buildSqlCondition value=invenoryGroupFiter /></#if> <#-- NEW facility group ids need to be passed for the groups on which routing is expected to be performed -->
  having
    <#if distance?has_content>distance <@buildSqlCondition value=distance /> and </#if>
    <#assign assignmentEnumId = orderRoutingRule.assignmentEnumId!"ORA_SINGLE"/>
    <#if brokeringSafetyStock?has_content>
        <#if 'ORA_SINGLE' == orderRoutingRule.assignmentEnumId> rank_by_order_above_facility_threshold='Y' <#-- NEW for sorting facility having all the items above threshold -->
        <#elseif 'ORA_MULTI' == orderRoutingRule.assignmentEnumId> item_at_facility_above_threshold='Y'</#if>
    <#else>
        <#if 'ORA_SINGLE' == orderRoutingRule.assignmentEnumId> rank_by_order_at_facility='Y' <#-- NEW for sorting facility having all the items above threshold -->
        <#elseif 'ORA_MULTI' == orderRoutingRule.assignmentEnumId> rank_by_order_at_facility='Y' OR rank_by_order_at_facility='N'</#if>
    </#if>
  order by
    <#if inventorySortByList?has_content>
        <#list inventorySortByList as inventorySortBy>
            ${inventorySortBy!}<#sep>,
        </#list>,
    </#if>
  rank_by_item_cnt_above_threshold desc, <#-- NEW -->
  availablity_pct DESC) as x
  cross join (select @rn:= 0,@r :=0,@oh :=0,@oi :=0, @f :=0,@ps :=0, @s :=0, @aq :=0,@rtd :=0,@fom :=0, @foc :=0, @fpuim :=0) as t
  ) as y
  where y.retained=1 and y.allocated_ord_qty <= y.ship_group_total_qty and y.facility_exhausted='N'
  order by y.row_num
</@compress>

<#--
  rank_by_order_at_facility: Ensures that shipGroup all items are located at a single facility, disregarding any threshold conditions.
  rank_by_order_above_facility_threshold: Ensures that shipGroup all items are located at a single facility and are available above a specified threshold.
  item_at_facility_above_threshold: Ensures that shipGroup items at any facility are available above a specified threshold.
  rank_by_item_cnt: Ranks shipGroup items based on the count of items, regardless of facility or threshold conditions.
  rank_by_item_cnt_above_threshold: Ranks shipGroup items based on the count of items, ensuring they are available above a specified threshold.
-->