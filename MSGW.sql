---------------------------------------------Gateway Facilities excluding AL and FL-------------------------------------
--------------------------------------Gateway Facilities::Aggregate Count-----------------------------------------------
select count(distinct hbr.facility_identifier)
from hbi_analytics.request_metrics as hbr
--join gateway.licensees as gwl on gwl.id = hbr.licensee_id
where hbr.request_successful = 't'
    --and gwl.name !~ 'Appriss'
    and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))  ---using one month
    and hbr.created_at::date < date_trunc('month', current_date)
    and hbr.facility_identifier is not null
    and hbr.originating_state not in ('AL','FL')


----------------------------Gateway Facilities:: count by Zip code--excluding AL and FL--------------------------------
select originating_state, zip as zipcode, count(distinct hbr.facility_identifier)
from hbi_analytics.request_metrics as hbr
join gateway.licensees as gwl on gwl.id = hbr.licensee_id
where hbr.request_successful = 't'
    --and gwl.name !~ 'Appriss'                    --Appriss is not in the gwl.name
    and hbr.originating_state not in ('AL','FL')
    and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))
    and hbr.created_at::date < date_trunc('month', current_date)
group by 1,2


------------------------Gateway Facilities::monthly facility count trend-excluding AL and FL----------------------------
select originating_state as state,TO_CHAR (hbr.created_at, 'YYYY-MM') as Date,
       count(distinct hbr.facility_identifier)
from hbi_analytics.request_metrics as hbr
--join gateway.licensees as gwl on gwl.id = hbr.licensee_id
where hbr.request_successful = 't'
    --and gwl.name !~ 'Appriss' --Why are we removing Apriss
    and Date is not NULL
    and hbr.originating_state not in ('AL','FL')
    and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))  ---using one month
    and hbr.created_at::date < date_trunc('month', current_date)
group by 1,2
order by Date desc;


-------------------------------Gateway Facilities::Aggregate Count from and to state excluding AL and FL----------------
----Aggregate:: Gateway requests from requesting state(from State) and disclosures from disclosing state (to States)----

with a as
(       ---from gateway
        select to_char(request_metrics.downstream_request_received_at, 'YYYY-MM') as mth,
                destination_metrics.destination as disclosing_state,
                request_metrics.originating_state as requesting_state,
                count(destination_metrics.id) as disclosures
        from gateway.request_metrics
        join gateway.destination_metrics on destination_metrics.request_metric_id = request_metrics.id
        where destination_metrics.response_type in ('PrescriptionData','NoData','Data','NarxScore')
        and disclosing_state is not NULL and requesting_state is not NULL
        and request_metrics.originating_state not in ('AL','FL')
         and request_metrics.downstream_request_received_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))
        and request_metrics.downstream_request_received_at::date < date_trunc('month', current_date)
        group by 1, 2, 3
        having disclosures >= 10
                                   union all
        ---from msgw
        select to_char(patient_response.created_at, 'YYYY-MM') as mth,
                patient_response.state_code as disclosing_state,
                patient_requests.location_address_state as requesting_state,
                count(patient_response.request_uuid) as disclosures
        from multisource_gateway.patient_response
        join multisource_gateway.patient_requests on patient_requests.request_uuid = patient_response.request_uuid

        where patient_response.response_type in ('PRESCRIPTIONDATA','NODATA','DATA','NARXSCORE')
        and
            disclosing_state is not NULL and requesting_state is not NULL
        and requesting_state not in ('AL','FL')
        and patient_response.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))
        and patient_response.created_at::date < date_trunc('month', current_date)
        group by 1, 2, 3
        having disclosures >= 10

        )

select dis_state as disclosing_state,req_state as requesting_state,sum(requests) as requests,sum(disclosures) as disclosures
from(
        select disclosing_state as dis_state, requesting_state as req_state, 0 as requests, sum(disclosures) as disclosures
        from a
        group by 1, 2
        union
        select requesting_state as dis_state, disclosing_state as req_state, sum(disclosures) as requests, 0 as disclosures
        from a
        group by 1, 2
    )
group by 1, 2
order by 1, 2;

--------------------------------does the state allow gateway- excluding AL and FL-----------------------------------------------
---Number of States allowing Gateway request_status:new_licensee 0, pending 1, granted 2, denied 3, auto_approved 4]

SELECT
    DISTINCT request_destinations.request_name,
             case when licensee_request_destinations.request_status ~ 2 then 'Yes'
        else 'No'
        end as state_allow_gateway
FROM
    gateway.licensee_request_destinations
JOIN
    gateway.request_destinations ON request_destinations.id = licensee_request_destinations.request_destination_id
WHERE
    licensee_request_destinations.request_status = 2
and request_destinations.request_name not in ('AL', 'FL')



--------------------------Gateway::Aggregate Count>> State using PMP Gateway excluding AL and FL-------------------------------------------
---Number of States allowing Gateway request_status:new_licensee 0, pending 1, granted 2, denied 3, auto_approved 4]*/
SELECT
    COUNT (DISTINCT request_destinations.request_name)
FROM
    gateway.licensee_request_destinations
JOIN
    gateway.request_destinations ON request_destinations.id = licensee_request_destinations.request_destination_id
WHERE
    licensee_request_destinations.request_status = 2
and request_destinations.request_name not in ('AL', 'FL')

---------------------Gateway Facilities::Facilities with PMP Gateway (same as 1) excluding AL and FL---------------------------------------------
select count(distinct hbr.facility_identifier)
from hbi_analytics.request_metrics as hbr
--join gateway.licensees as gwl on gwl.id = hbr.licensee_id
where hbr.request_successful = 't'
    --and gwl.name !~ 'Appriss'                  --Appriss is not in the gwl.name
    and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))  ---using one month
    and hbr.created_at::date < date_trunc('month', current_date)
    and hbr.facility_identifier is not null
    and hbr.originating_state not in ('AL','FL')


-----------Gateway::Aggregate Count >> Provider (Prescribers and Pharmacists)
select case when hbr.requester_role ~ 'Pharmacist' then 'Pharmacists'
        else 'Prescribers'
        end as user_role,
        count(distinct hbr.requester_identifier)
    from hbi_analytics.request_metrics as hbr
    where hbr.request_successful = 't'
    --and gwl.name !~ 'Appriss'
    and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))  ---using one month
    and hbr.created_at::date < date_trunc('month', current_date)
    and hbr.originating_state not in ('AL','FL')
    group by 1;

--------------------------Gateway::Aggregate Count >>  Patient Encounters Per MONTH------------------------------------
--------------------------------excluding AL and FL-----------------------------
SELECT
    TO_CHAR (hbr.created_at, 'YYYY-MM'),
    COUNT (hbr.id)
    FROM hbi_analytics.request_metrics as hbr
    WHERE
    hbr.requestable_type IN ('SearchRequest',
                                         'NcpdpRequest',
                                         'NarxScoreRequest', --firstcall
                                         'PatientRequest')
--AND request_metrics.downstream_request_received_at::DATE BETWEEN '2022-06-01' AND '2022-12-31'
   and hbr.created_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))  ---using one month
    and hbr.created_at::date < date_trunc('month', current_date)
    AND hbr.request_successful = true
    and hbr.originating_state not in ('AL','FL')
    GROUP BY 1
    ORDER BY 1;


---------------------------------------------Requests and Disclosures---------------------------------------------------
---------Values for PMP Interconnect total transactions for each month excluding AL and FL------------------------------

SELECT
    COUNT (DISTINCT search_request_metrics.search_request_id) as requests,
    COUNT (search_request_metrics.disclosure_request_id) as disclosures,
    requests + disclosures as ineterconnect_total_trx
FROM
    pmpi.search_request_metrics
WHERE
    search_request_metrics.searching_pmp_id not in  (255,126,257) --Appriss Test PMP,Alabama,florida
and search_request_metrics.search_request_received_at::date >= DATEADD(MONTH, -1, date_trunc('month', current_date))
and search_request_metrics.search_request_received_at::date < date_trunc('month', current_date)
AND search_request_metrics.disclosure_successful = TRUE

;

---------------------PDMPs Sharing on PMP Interconnect excluding AL and FL----------------------------------------------

SELECT
   COUNT(DISTINCT CASE WHEN pmps.display_name ~* 'Maryland' THEN 'Maryland' ELSE pmps.display_name END)
FROM
    pmpi.pmps
WHERE
    pmps.id NOT IN (45, --Regenstrief
                    46, --DrFirst
                    47, --Emdeon HIE
                    86, --C4Uh (HIE)
                    88, --Via Christi Health
                    90, --Narxcheck
                    126, --Alabama
                    196, --NDHIN
                    198, --MHIN
                    199, --Gateway
                    237, --LACIE
                    255, --Appriss Test PMP
                    257, --Florida
                    390) --Gateway FedRamp
AND pmps.active = TRUE;


