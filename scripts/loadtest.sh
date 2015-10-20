#!/bin/sh

ab -n 2000 -c 8 https://data-staging.ripple.com/v2/transactions &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/ledgers?expand=true &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/accounts &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/accounts?parent=r4cgRZUJs7sAXsM6ykTCHhYAPBwsRGDGEv &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/accounts/r4cgRZUJs7sAXsM6ykTCHhYAPBwsRGDGEv/transactions &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/accounts/r4cgRZUJs7sAXsM6ykTCHhYAPBwsRGDGEv/payments &
ab -n 2000 -c 8 https://data-staging.ripple.com/v2/ledgers
