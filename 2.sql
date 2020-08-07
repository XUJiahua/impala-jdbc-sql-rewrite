select a.dt
,(a.x - b.x) as `净增人数`
from
(
SELECT dt, count(distinct userinfo_openid) as x
FROM f_ods_wx_subscribe_di
WHERE mid = ?
AND dt >= ?
and dt < ?
group by dt
) a
left join
(
SELECT dt, count(distinct userinfo_openid) as x
FROM f_ods_wx_unsubscribe_di
WHERE mid = ?
AND dt >= ?
and dt < ?
group by dt
) b
on a.dt = b.dt