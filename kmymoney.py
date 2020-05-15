import pandas as pd


class ACCOUNT_TYPE:
    INCOME = '12'
    EXPENSE = '13'
    STOCK = '15'
    EQUITY = '16'


DEFAULT_CURRENCY = 'EUR'   # ??? should be computed from FileInfo.baseCurrency


class KMyMoney:
    """
    A python interface to KMyMoney SQL files.

    This assumes your file was saved using the SQL format, not the XML format.
    It provides a number of high-level queries to create reports using the data
    from this file.

    This is a read-only interface to the file, always edit it via KMyMoney.
    As such, it is safe to use this script in parallel with a KMyMoney session.
    """

    def __init__(self, filename):
        self.sqlite = 'sqlite:///{}'.format(filename)

    def _qaccount_name(self):
        """
        Return the Common Table Expression to compute the fully qualified
        account names, including the parent accounts.
        The contents is similar to kmmAccounts, but account names are also
        available as:
            Assets:Toplevel:Parent:Child
        """
        return """
        qAccountName(
           accountId, accountType, name, accountName, currencyId
        ) AS (
            SELECT kmmAccounts.id, kmmAccounts.accountType,
                   kmmAccounts.accountName, kmmAccounts.accountName,
                   kmmAccounts.currencyId
               FROM kmmAccounts
               WHERE kmmAccounts.parentId IS NULL
            UNION ALL
            SELECT kmmAccounts.id, kmmAccounts.accountType,
                   qAccountName.name || ':' || kmmAccounts.accountName,
                   kmmAccounts.accountName, kmmAccounts.currencyId
               FROM qAccountName, kmmAccounts
               WHERE kmmAccounts.parentId = qAccountName.accountId
        )
        """

    def _to_float(self, fieldname):
        """
        Converts a "n/m" value as stored by KMyMoney to a float.
        The `*Formatted` field in the database seem wrong sometimes.
        """
        return (
            f"1.0 * substr({fieldname}, 0, instr({fieldname}, '/'))"
            f"/substr({fieldname}, instr({fieldname}, '/') + 1)"
        )

    def _splits_and_fees(self, currency=DEFAULT_CURRENCY):
        """
        return the Common Table Expression to compute the list of
        all transactions include their fees. This works for both checking
        transactions (which have no fee) and transactions on investments.

        This function also computes quantities, prices and values from
        the "n/m" fields in the database, since the equivalent sharesFormatted,
        priceFormatted and valueFormatted seem to be wrong sometimes.

        NOTE:
        Only fees in the given currency are taken into account (so if you
        have a mix of currencies for those fees, some will be ignored).
        """
        return f"""splits_and_fees AS (
       SELECT
          s.transactionId,
          s.splitId,
          s.accountId,
          s.action,
          {self._to_float('s.shares')} as quantity,
          {self._to_float('s.price')} as price,
          {self._to_float('s.value')} as value,
          s.postDate,
          SUM({self._to_float('t.value')}) as fees
       FROM
          kmmSplits s
          JOIN kmmAccounts ON (s.accountId = kmmAccounts.id)
          LEFT JOIN
            (kmmSplits
             JOIN kmmAccounts ta
             ON (
                kmmSplits.accountId = ta.id

                --  fees are computed on Expense accounts
                --  ??? This is approximate
                AND ta.accountType = {ACCOUNT_TYPE.EXPENSE}

                AND ta.currencyId = "{currency}"
             )
         ) t
         ON (s.transactionId = t.transactionId
             AND s.splitId != t.splitId

             --   Fees only apply to Stock accounts
             AND kmmAccounts.accountType = {ACCOUNT_TYPE.STOCK}
             )
       GROUP BY 1,2,3,4,5,6,7,8
    )
         """

    def _price_history(
        self,
        currency,
    ):
        """
        Return the common table expression to compute the price history for
        stocks, including the full time range that this price
        applies.
        """
        return f"""price_history AS (
       SELECT kmmPrices.*,
          {self._to_float('price')} as computedPrice,
          (
             SELECT COALESCE(MIN(priceDate), '9000-01-01')
             FROM kmmPrices m
             WHERE kmmPrices.fromId = m.fromId
               AND m.priceDate > kmmPrices.priceDate
          ) as maxDate
       FROM kmmPrices
       WHERE kmmPrices.toId = "{currency}"
      )
        """

    def _test_accounts(self, tablename="kmmSplits", accounts=None):
        """
        Restrict a query to a specific set of accounts.
        :param accounts:
           either None (all accounts), a string for the name of a single
           account, or a list of account ids.
        """
        if isinstance(accounts, (list, tuple)):
            return (
                f" AND {tablename}.accountid in (%s)"
                % ",".join("'%s'" % a for a in accounts)  # ??? unsafe
            )
        elif accounts:
            return f" AND {tablename}.accountid = '{accounts}'"
        else:
            return ""

    def networth(
        self,
        accounts=None,
        currency=DEFAULT_CURRENCY,
        by_year=False,
        mindate=None,
        maxdate=None,  # "2020-12-31"  (end of period)
        with_total=True,
    ):
        """
        Compute the networth for all accounts at the end of each month or year
        in the given date range. The result is a pivot table.
        """

        if by_year:
            # this must be the first date in the file with transactions, since
            # balances are computed by adding all transactions from the
            # beginning of times.
            # ??? should be computed
            # It should be set to end of the periods
            earliest_date = "2009-12-31"
            inc = "+1 YEARS"
            formatted = "%Y"

        else:
            earliest_date = "2009-01-01"
            inc = "+1 MONTHS"
            formatted = "%Y-%m"

        p = pd.read_sql(
            f"""
        WITH RECURSIVE

        --  Generate the set of dates for which we want balances
        all_dates(d) AS (
            SELECT "{earliest_date}"
            UNION ALL
            SELECT DATE(d, "{inc}") FROM all_dates
               WHERE DATE(d, "{inc}") <= "{maxdate}"
        ),

        --  We will need to compute the prices at the end of each period
        {self._price_history(currency)},

        --  The full list of accounts, including fully qualified names
        {self._qaccount_name()},

        --  For each account, the balanceShare after the last transaction
        --  during each year. If there was no transaction, NULL is returned
        balances AS (
           SELECT DISTINCT
               strftime("{formatted}", s.postDate) as date,
               s.accountId,
               LAST_VALUE(s.balanceShares)
                  OVER (PARTITION BY strftime("{formatted}", s.postDate),
                                     s.accountId
                        ORDER BY s.postDate ASC
                        RANGE BETWEEN UNBOUNDED PRECEDING
                          AND UNBOUNDED FOLLOWING
                  ) as balanceShares
           FROM
              (SELECT
                  s.postDate,   --  ??? Could use LAST_VALUE, and a GROUP BY
                  s.accountId,
                   --  compute a running total of shares, per account
                  SUM({self._to_float('s.shares')})
                    OVER (PARTITION BY s.accountid
                          ORDER BY s.postDate
                          ROWS BETWEEN UNBOUNDED PRECEDING
                             AND CURRENT ROW
                             --  AND UNBOUNDED FOLLOWING
                         ) as balanceShares
               FROM kmmSplits s
              ) s
        )

        SELECT all_dates.d as date,
           qAccountName.name as accountname,
           qAccountName.accountId,
           balances.balanceShares as balanceShares,
           coalesce(price_history.computedPrice, 1) as computedPrice
        FROM all_dates
          JOIN qAccountName
          LEFT JOIN balances
            ON (strftime("{formatted}", all_dates.d) = balances.date
                AND qAccountName.accountId = balances.accountId
               )
          LEFT JOIN price_history
            ON (price_history.fromId = qAccountName.currencyId
                AND all_dates.d >= price_history.priceDate
                AND all_dates.d < price_history.maxDate
            )

        WHERE qAccountName.accountType not in (:expense, :income, :equity)
           {self._test_accounts('qAccountName', accounts)}
            """,
            self.sqlite,
            params={
                "expense": ACCOUNT_TYPE.EXPENSE,
                "income": ACCOUNT_TYPE.INCOME,
                "equity": ACCOUNT_TYPE.EQUITY,
            }
        )

        pivot = pd.pivot_table(
            p,
            values=['balanceShares', 'computedPrice'],
            index=['accountname'],
            columns=['date'],
            dropna=False,
        )

        # ??? If all balances are None, the column disappears
        if 'balanceShares' not in pivot.columns:
            return None

        # Replace NaN with the last known value in the same row (propagate
        # past balances)
        pivot = pivot.groupby(level=0).fillna(axis=1, method='ffill')

        # Compute the EUR balance, taking the number of shares of the last
        # transaction, with the price computed at the end of each period.
        p = pivot['balanceShares'] * pivot['computedPrice']

        # Only remove old columns after we have propagated values
        if mindate:
            p = p[[c for c in p.columns if c >= mindate]]

        if with_total:
            p =  pd.concat([
                p,
                # pivot.sum(level=0).assign(accountname='Subtotal')
                #   .set_index('accountname', append=True),
                p.sum().to_frame().T
                   .assign(accountname='Total')
                   .set_index(['accountname'])
            ]).sort_index()

        return p

    def _query_detailed_splits(
        self,
        accounts=None,
        currency=DEFAULT_CURRENCY,
        maxdate=None,     # "1900-01-01"
    ):
        """
        A query that returns data similar to kmmSplits, but each split has
        detailed compatible with both checkins transactions and investment
        transactions:
           `quantity`: the number of units exchanged, in the account's
              currency. For a checking account, this will be EUR for instance,
              and for a stock account it will be the number of shares.
           `price`: the price of each unit for this transaction. For a
              checking account, it will be "1" in general. For a stock account
              it will be the price per share, or null in case of
              "add/remove shares".
           `value`: the value of the transaction (typically quantity*price),
              which does not include any fee paid to a third party.
           `fees`: extra amount paid for banking fees. This is only set for
              stock accounts, and null otherwise.
           `computedPrice`: the price of the unit as of the transaction. This
              is either the price actually used in the transaction, or a price
              coming from the price history.
           `balanceShares`: the current amount of units in the account (i.e.
              the money in the account for a checking account, or the number of
              shares for a stock account)
        """
        test_max_date = "" if maxdate is None else " AND s.postDate <= :maxdate"
        return f"""
        WITH RECURSIVE
        {self._splits_and_fees(currency)},
        {self._price_history(currency)},
        {self._qaccount_name()}
        SELECT
           kmmAccounts.id as accountId,
           kmmAccounts.currencyId as currencyId,
           s.postDate as date,
           s.transactionId,
           s.splitId,
           s.quantity,
           s.price,
           s.value,
           s.fees,
           kmmSecurities.id as securityId,

           --  price is either from the transaction, or from the historical
           --  prices. If the account is already in the proper currency, price
           --  defaults to 1
           COALESCE(s.price, price_history.computedPrice,
                    CASE kmmAccounts.currencyId
                       WHEN "{currency}" THEN 1
                       ELSE NULL
                    END
           ) as computedPrice,

           --  compute a running total of shares, per account
           SUM(s.quantity) OVER (
               PARTITION BY s.accountid ORDER BY s.postDate
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) as balanceShares
        FROM splits_and_fees s
          JOIN kmmAccounts ON (kmmAccounts.id = s.accountId)
          LEFT JOIN kmmSecurities ON (kmmSecurities.id = kmmAccounts.currencyId)
          LEFT JOIN price_history ON
             (kmmSecurities.id = price_history.fromId
              AND s.postDate >= price_history.priceDate
              AND s.postDate < price_history.maxDate)
        WHERE TRUE{self._test_accounts('s', accounts)}{test_max_date}
        ORDER BY postDate
            """

    def ledger(
        self,
        accounts=None,
        currency=DEFAULT_CURRENCY,
        mindate=None,     # "1900-01-01"
        maxdate=None,     # "1900-01-01"
    ):
        """
        Compute the list of transactions in a given account, with an output
        similar to kMyMoney. It reports data both in the account's currency
        (EUR for checking accounts for instance, or number of shares for a
        stock), and in EUR, using the historical prices to value the position
        at the time.
        
        A split transaction (ie the money is split into multiple destination
        accounts) will result in multiple lines, with the same balance.
        For instance:
            payee    shares   balanceShares   paiement   deposit  balance
            Kraken   0.01     0.03               -         60.1    1200
            Bank     0.01     0.03              0.1          -     1200
        """
        test_min_date = "" if mindate is None else " AND s.date >= :mindate"
        q = self._query_detailed_splits(
            accounts=accounts, currency=currency, maxdate=maxdate)
        return pd.read_sql(
            f"""
            WITH RECURSIVE {self._qaccount_name()}
            SELECT
               qAccountName.name as accountName,
               s.date,
               coalesce(payee.name, '') as payee,
               destAccount.accountName as category,
               (CASE kmmSplits.reconcileFlag
                  WHEN '2' THEN 'R' WHEN '1' THEN 'C' ELSE '' END
               ) as reconcile,
               s.quantity as shares,  --  in account's currency (EUR, shares,...)
               s.balanceShares as balanceShares,  --  in account's currency
               s.price as pricePerShare,
               (CASE WHEN destS.value > 0 THEN {self._to_float('destS.value')}
                     ELSE NULL END) as paiement,
               (CASE WHEN destS.value <= 0 THEN -{self._to_float('destS.value')}
                     ELSE NULL END) as deposit,
               s.balanceShares * s.computedPrice as balance
            FROM ({q}) s
               JOIN qAccountName using (accountId)
               JOIN kmmSplits using (transactionId, splitId)
               LEFT JOIN kmmSplits destS on
                  (s.transactionId = destS.transactionId
                   AND s.splitId != destS.splitId)
               LEFT JOIN kmmAccounts destAccount
                  ON (destS.accountId = destAccount.id)
               LEFT JOIN kmmPayees payee on (kmmSplits.payeeId = payee.id)
            WHERE TRUE{test_min_date}
            """,
            self.sqlite,
            params={
                "mindate": mindate,
            }
        )

    def plot_by_category(
        self,
        accounts=None,
        currency=DEFAULT_CURRENCY,
        mindate=None,
        maxdate=None,
        values=['paiement'],  # could include 'deposit'
        expenses=True,   # or Income
        kind="pie",
        subplots=True,   # True if each entry in `values` should be a subplot
    ):
        """
        Generate a plot of paiements and deposits by category, for the given
        time range.  Those are not the same as Expense and Income, because
        nothing prevents us from doing a deposit in an Expense account, for
        instance (e.g. a reimbursement for some earlier expense)
        """
        test_min_date = "" if mindate is None else " AND s.date >= :mindate"
        q = self._query_detailed_splits(
            accounts=accounts, currency=currency, maxdate=maxdate)
        p = pd.read_sql(
            f"""
            SELECT
               destAccount.accountName as category,
               destAccount.accountType as categorytype,
               (CASE WHEN destS.value > 0 THEN {self._to_float('destS.value')}
                     ELSE NULL END) as paiement,
               (CASE WHEN destS.value <= 0 THEN -{self._to_float('destS.value')}
                     ELSE NULL END) as deposit,
               -{self._to_float('destS.value')} as amount
            FROM ({q}) s
               JOIN kmmSplits destS on
                  (s.transactionId = destS.transactionId
                   AND s.splitId != destS.splitId)
               JOIN kmmAccounts destAccount
                  ON (destS.accountId = destAccount.id)
            WHERE TRUE{test_min_date}
               AND destAccount.accountType IN (:income, :expense)
            """,
            self.sqlite,
            params={
                "mindate": mindate,
                "income": ACCOUNT_TYPE.INCOME,
                "expense": ACCOUNT_TYPE.EXPENSE,
            }
        )

        if not p.empty:
            # Group by category and sum the amounts
            p = p[['category'] + list(values)].groupby(['category']).sum()
            p = p.sort_values(values)
            
            if kind == 'pie':
                params = dict(
                    # no "y" parameter, this fails with pie plots
                    autopct="%.2f%%",
                    kind='pie',
                    subplots=True,
                )
            else:
                params = dict(
                    y=values,
                    kind=kind,
                    subplots=subplots,
                )
            
            p.plot(
                **params,
                title="{} - {}".format(mindate or "", maxdate or ""),
                legend=None,
                figsize=(20, 10),
                logy=False
            )

    def price_history(self, accounts=None):
        """
        Return a pivot table with a price history for investment accounts
        """
        p = pd.read_sql(
            f"""
            WITH RECURSIVE
            {self._price_history('EUR')},
            {self._qaccount_name()}
            SELECT priceDate as date, computedPrice as price,
                qAccountName.accountName as name
            FROM price_history,
               qAccountName
            WHERE 
               qAccountName.currencyId = price_history.fromId
               AND qAccountName.accountType = 15
               {self._test_accounts('qAccountName', accounts)}
            """,
            self.sqlite,
        )
        return pd.pivot_table(
            p,
            values='price',
            index=['date'],
            columns=['name'],
        )