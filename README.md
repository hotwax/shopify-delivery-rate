# Shopify Delivery Rate

A carrier service (also known as a carrier-calculated service or shipping service) provides real-time shipping rates to Shopify. The term carrier is often used interchangeably with shipping company or rate provider.
Using the CarrierService resource, you can add a carrier service to a Shopify shop, which provides a list of applicable shipping rates at checkout. You can also leverage cart data to adjust shipping rates and offer discounts based on the contents of the customer's cart.

## Overview

The Shopify Delivery Rate API integrates with your commerce system to compute delivery rates based on the OrderRoutingGroup setup and returns the estimated delivery date in the Shopify format.

Endpoint
`GET /rest/s1/shopify-delivery/carrier/rate`

This endpoint accepts a Shopify shipping rate request and uses the destination address's latitude/longitude for inventory filtering. If latitude/longitude is not present, the system fetches it from the commerce postal code database based on the country and postal code.

### Shipping Rules

#### Standard Shipping:
- Distance Lead Time: Determines the nearest fulfillment location with fillable inventory:
    ```
    < 150 miles: +1 day
    < 300 miles: +2 days
    < 600 miles: +3 days
    > 600 miles: +5 days
    ```

- Operations Lead Time: Considers the day of the week and fulfillment facility type:
  ```
    Monday - Friday: +24 hours
    Saturday:
    Warehouse: +24 hours
    Store: +48 hours
    Sunday: +48 hours

    Total Lead Time: current date + Distance Lead Time + Operations Lead Time
    ```
####  Expedited Shipping:
- Distance Lead Time: Derived from the CarrierShipmentMethod entity:
    ```
    NEXT_DAY: 1 day
    SECOND_DAY: 2 days
    STANDARD: Varies
    Operations Lead Time:
    Time of Day:
    < 12:00 PM PST: +0 hours
    > 12:00 PM PST: +24 hours
    Day of the Week:
    Monday - Friday: +0 hours
    Saturday:
    Warehouse: +0 hours
    Store: +48 hours
    Sunday: +24 hours
  ```
Examples
```
<CarrierShipmentMethod carrierServiceCode="NEXT_DAY" deliveryDays="1" partyId="_NA_" roleTypeId="CARRIER" sequenceNumber="10" shipmentMethodTypeId="NEXT_DAY"/>
<CarrierShipmentMethod carrierServiceCode="SECOND_DAY" deliveryDays="2" partyId="_NA_" roleTypeId="CARRIER" sequenceNumber="10" shipmentMethodTypeId="SECOND_DAY"/>
<CarrierShipmentMethod carrierServiceCode="STANDARD" deliveryDays="" partyId="_NA_" roleTypeId="CARRIER" sequenceNumber="10" shipmentMethodTypeId="STANDARD"/>

```
 
#### Shipment Method Cost Calculation

The system uses the ShipmentCostEstimate entity to calculate shipping costs, using the ShipmentCostEstimate.orderFlatPrice as the final shipping price.

Examples:
```
<ShipmentCostEstimate carrierPartyId="_NA_" carrierRoleTypeId="CARRIER" orderFlatPrice="0.00" productStoreId="STORE" shipmentCostEstimateId="STANDARD" shipmentMethodTypeId="STANDARD"/>
<ShipmentCostEstimate carrierPartyId="_NA_" carrierRoleTypeId="CARRIER" orderFlatPrice="10.00" productStoreId="STORE" shipmentCostEstimateId="SECOND_DAY" shipmentMethodTypeId="SECOND_DAY"/>
<ShipmentCostEstimate carrierPartyId="_NA_" carrierRoleTypeId="CARRIER" orderFlatPrice="20.00" productStoreId="STORE" shipmentCostEstimateId="NEXT_DAY" shipmentMethodTypeId="NEXT_DAY"/>
```