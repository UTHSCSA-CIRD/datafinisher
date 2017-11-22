create table if not exists df_dtdict as 
/*
select distinct cid,ddomain,ccd,'' mxinsts,'' mxfacts,
'v'||substr('000'||cid,-3) colcd
-- the below are semi-human-readable, unique, and relatively short column names
--,'v'||substr('000'||cid,-3)||'_'||replace(trim(drl(shw(name,15))),' ','_') colid
,'v'||substr('000'||cid,-3)||'_'||replace(trim(name,15),' ','_') colid
,concept_path,name,mod,tval_char,nval_num,valueflag_cd,units_cd,confidence_num
,quantity_num,location_cd,valtype_cd,0 done
,'UNKNOWN_DATA_ELEMENT' rule
from 
(select df_codeid.id cid,group_concat(distinct ddomain) ddomain,count(distinct ccd) ccd,concept_path,name
from df_codeid left join variable on df_codeid.id = variable.id
group by df_codeid.id) dfc
left join
(select id
,count(distinct case when modifier_cd = '@' then null else modifier_cd end) mod
,count(distinct case when tval_char in ('E','TNP') then null else tval_char end) tval_char
,count(nval_num) nval_num
,count(distinct valueflag_cd) valueflag_cd
,count(distinct units_cd) units_cd
,count(distinct confidence_num) confidence_num
,count(distinct quantity_num) quantity_num
,count(distinct location_cd) location_cd
,group_concat(distinct valtype_cd) valtype_cd
from df_obsfact group by id) dfo
on cid = id
;
*/


select distinct df_codeid.*,mxinsts,mxfacts,'v'||substr('000'||cid,-3) colcd
-- the below are semi-human-readable, unique, and relatively short column names
,'v'||substr('000'||cid,-3)||'_'||replace(trim(drl(shw(name,15))),' ','_') colid
,concept_path,name,mod,tval_char,nval_num,valueflag_cd,units_cd,confidence_num
,quantity_num,location_cd,valtype_cd,0 done
,'UNKNOWN_DATA_ELEMENT' rule
from (select df_codeid.id cid,group_concat(distinct ddomain) ddomain,count(distinct ccd) ccd from df_codeid group by id) df_codeid
left join variable on cid = variable.id
left join (
select count(distinct modifier_cd) mod,id from df_obsfact
where modifier_cd is not null 
and modifier_cd != '@'
group by id) mdid on cid = mdid.id
left join (
select count(distinct tval_char) tval_char,id from df_obsfact
where tval_char is not null and tval_char not in ('E','TNP')
group by id) tvid on cid = tvid.id
left join (
select 1 nval_num,id from df_obsfact
where nval_num is not null
order by id) nvid on cid = nvid.id
left join (
select count(distinct valueflag_cd) valueflag_cd,id from df_obsfact
where valueflag_cd is not null 
group by id) vfid on cid = vfid.id
left join (
select count(distinct units_cd) units_cd,id from df_obsfact
where units_cd is not null
group by id) unid on cid = unid.id
left join (
select count(distinct confidence_num) confidence_num,id from df_obsfact
where confidence_num is not null
group by id) cnid on cid = cnid.id
left join (
select count(distinct quantity_num) quantity_num,id from df_obsfact
where quantity_num is not null
group by id) qnid on cid = qnid.id
left join (
select count(distinct location_cd) location_cd,id from df_obsfact
where location_cd is not null
and location_cd != '@'
group by id) loid on cid = loid.id
left join (
select group_concat(distinct valtype_cd) valtype_cd,id from df_obsfact
where valtype_cd is not null
group by id) vtid on cid = vtid.id
left join (
select id,max(cnt) mxinsts from (
        select id,pn,sd,count(*) cnt
        from df_obsfact group by pn,sd,id
) group by id) counts on cid = counts.id
left join (
select id,max(cnt) mxfacts from (
        select id,pn,sd,count(*) cnt
        from df_obsfact group by pn,sd,id
) group by id) fcounts on cid = fcounts.id
