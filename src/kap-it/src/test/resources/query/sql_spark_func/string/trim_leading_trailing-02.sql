select "name",trim(LEADING 'lanA' from "name"),trim(TRAILING 'lanA' from "name")
from "DEFAULT".TEST_COUNTRY  order by "name" limit 500;