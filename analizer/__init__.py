# Common import 
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# Settings And Const
TYPE_FOR_BALANCE = "balance"
COMMENT_PATTERN_FOR_ORDERS_ONLY = "[tp]"
COMMENT_PATTERN_FOR_DEPOSIT = '(Deposit|Transfer|Withdraw)' # Withdraw is included for cancelation transaction deposit
COMMENT_PATTERN_FOR_WITHDRAWAL = "Withdraw"
COMMENT_PATTERN_FOR_CANCELED = "can"
COMMENT_PATTERN_FOR_START_ORDERS = 'Start'
COMMENT_PATTERN_FOR_SELL_ORDERS = 'SELL'
COMMENT_PATTERN_FOR_BUY_ORDERS = 'BUY'

OPEN_NEW_ORDER_PRICE_DELTA_PIPS = 180 # Eve average position when price goes more then 180 pips
XAU_PIP_USD = 0.01 # 1 pip = 0.01 for XAU

QTY_FACTOR_FOR_AVERAGE = 1.65 #todo Must use table
EVE_MAX_ORDER_COUNT = 20

COLUMNS_MAPPING={
    'Deal': 'ORDER_ID', 
    'Open time': 'OPEN_DT', 
    'Type': 'SIDE', 
    'Size': 'QTY', 
    'Item': 'SYMBOL', 
    'Price': 'OPEN_PRICE', 
    'S/L': 'STOP_LOSS', 
    'T/P': 'TAKE_PROFIT',
    'Time': 'CLOSE_DT', 
    'Close': 'CLOSE_PRICE', 
    'Commission': 'FEE', 
    'Swap': 'SWAP', 
    'Profit': 'PROFIT', 
    'Comment': 'COMMENT'
}


def set_grid_id_by_one_side_start(df: pd.DataFrame, grid_id_col_name: str) -> pd.DataFrame:
    '''
        Set GRID_ID for all orders in df.
        New GRID_ID generates when order has "Start" mark inside COMMENT. 
        And it applies for all next orders of the same side until new "Start" mark comes.

        It's BEST method for one side grids.
    '''

    df_res = df.copy()

    net_id_sell, net_id_buy = None, None
    net_id_list = []
    for i, row in df_res.iterrows():
        if COMMENT_PATTERN_FOR_START_ORDERS in row['COMMENT']:
            if COMMENT_PATTERN_FOR_SELL_ORDERS in row['COMMENT']:
                net_id_sell = row['ORDER_ID']
            if COMMENT_PATTERN_FOR_BUY_ORDERS in row['COMMENT']:
                net_id_buy = row['ORDER_ID']

        if COMMENT_PATTERN_FOR_SELL_ORDERS in row['COMMENT']:
            net_id_list.append(net_id_sell if net_id_sell is not None else row['ORDER_ID']) 
        if COMMENT_PATTERN_FOR_BUY_ORDERS in row['COMMENT']:
            net_id_list.append(net_id_buy if net_id_buy is not None else row['ORDER_ID']) 
        
    df_res[grid_id_col_name] = net_id_list

    return df_res

def set_grid_id_by_timeline_emulating(df: pd.DataFrame, grid_id_col_name: str) -> pd.DataFrame:
    '''
        Set GRID_ID for all orders in df.
        Def emulates timeline using OPEN_DT and CLOSE_DT each order.
        New GRID_ID generates then first order opens. 
        It applies for all next orders until net has at least 1 order.
        When net becomes empty new GRID_ID will be generate.

        Pros: Usually one side grid closes when opposite side is still open. And this situation repeats often.
              So grids become huge - more than 20 orders.
              I guess that grid is order with same side.
              But grids with different sides can be opened at one time.
              So why this method is no good.
    '''    

    df_res = df.copy()
    df_timeline = pd.DataFrame(columns=['ORDER_ID', 'DT', 'EVENT']) # Event 0-open, 1-close

    # Place orders at open and close timeline
    for order_id, open_dt, close_dt in zip(df_res['ORDER_ID'], df_res['OPEN_DT'], df_res['CLOSE_DT']):
        event = {'ORDER_ID': order_id, 'DT': open_dt, 'EVENT': 0}
        df_timeline.loc[len(df_timeline)] = event

        event = {'ORDER_ID': order_id, 'DT': close_dt, 'EVENT': 1}
        df_timeline.loc[len(df_timeline)] = event

    df_timeline = df_timeline.sort_values(['DT', 'EVENT'], ascending=True)

    # Emulates opening and closing deal in net
    grid_curr = []
    grid_full = []
    grid_id = None
    for i, row in df_timeline.iterrows():
        if len(grid_curr) == 0:
            if grid_id is not None:
                df_res.loc[df_res['ORDER_ID'].isin(grid_full), [grid_id_col_name]] = grid_id
                grid_full = []

            grid_id = row['ORDER_ID']

        if row['EVENT'] == 0:
            grid_curr.append(row['ORDER_ID'])
            grid_full.append(row['ORDER_ID'])
        else:
            grid_curr = list(filter(lambda i: i == row['ORDER_ID'], grid_curr))

    return df_res


def set_cum_of_grids(df: pd.DataFrame, group_by_col: str, col_for_cum: str,
                     res_col_name: str) -> pd.DataFrame:
    '''
        Calculate cumulative value by col_for_cum grouping by group_by_col
    '''

    df_res = df.copy()
    df_res[res_col_name] = df_res.groupby(group_by_col)[col_for_cum].transform(pd.Series.cumsum)
    return df_res


def set_worst_grid_price(df: pd.DataFrame, new_col_name: str) -> pd.DataFrame:
    '''
        Set for every order his worst price.
        Worst price is price lower then 1 pips of OPEN_NEW_ORDER_PRICE_DELTA_PIPS.
        If it was equal OPEN_NEW_ORDER_PRICE_DELTA_PIPS EVE took an new order.
    '''
    df_res = df.copy()

    df_res = df_res.sort_values(by=['DK_GRID_ID', 'ORDER_ID'], ascending=False)
    worst_price_list = []
    net_id_prev = None
    open_price_prev = None
    for (net_id, open_price, side) in zip(df_res['DK_GRID_ID'], df_res['OPEN_PRICE'], df_res['SIDE']):
        if net_id == net_id_prev:
            worst_price_list.append(open_price_prev)
        else:
            max_price_delta = (OPEN_NEW_ORDER_PRICE_DELTA_PIPS - 1) * XAU_PIP_USD
            if side == 'buy': 
                max_price_delta = -1 * max_price_delta
            worst_price_list.append(open_price + max_price_delta)

        open_price_prev = open_price
        net_id_prev = net_id

    df_res[new_col_name] = worst_price_list
    return df_res


def prepare_columns(df: pd.DataFrame) -> pd.DataFrame:
    ''' 
    Renames columns using COLUMNS_MAPPING.
    Check all columns in df after, overwise call Exception/.
    '''
    df_res = df.copy()
    df_res = df_res.rename(columns=COLUMNS_MAPPING)
    if not all([c in df_res.columns for c in COLUMNS_MAPPING.values()]):
        raise Exception("There're not all columns from COLUMNS_MAPPING in data frame")
    
    # # Exclude CANCELLED transactions
    # df_res = df_res[df_res['COMMENT'].str.contains(COMMENT_PATTERN_FOR_CANCELED, regex=False) == False]
        
    # Datetime casting
    df_res['OPEN_DT'] = pd.to_datetime(df_res['OPEN_DT'])
    df_res['CLOSE_DT'] = pd.to_datetime(df_res['CLOSE_DT'])

    df_res = df_res.sort_values(by=['OPEN_DT', 'ORDER_ID'])

    # Convert USDC to USD
    df_res['PROFIT'] = df_res['PROFIT'] / 100
    df_res['DK_TRANS'] = df_res['PROFIT']
    df_res['DK_MISC_TRANS'] = 0

    # Add columns
    df_res['DK_DEPOSIT'] = 0
    df_res['DK_WITHDRAWAL'] = 0
    
    df_res.loc[(df_res['SIDE'] == TYPE_FOR_BALANCE) & (df_res['PROFIT'] > 0) & (df_res['COMMENT'].str.contains(COMMENT_PATTERN_FOR_DEPOSIT)), ['DK_DEPOSIT']] = df_res['PROFIT']
    df_res.loc[(df_res['SIDE'] == TYPE_FOR_BALANCE) & (df_res['PROFIT'] < 0), ['DK_WITHDRAWAL']] = df_res['PROFIT']
    df_res.loc[(df_res['SIDE'] == TYPE_FOR_BALANCE) & (df_res['PROFIT'] < 0), ['DK_WITHDRAWAL']] = df_res['PROFIT']

    df_res['DK_PROFIT_2'] = 0
    df_res.loc[(df_res['SIDE'] != TYPE_FOR_BALANCE), ['DK_PROFIT_2']] = df_res['PROFIT']
    df_res['DK_MISC_TRANS'] = df_res['PROFIT'] - (df_res['DK_DEPOSIT'] + df_res['DK_WITHDRAWAL'] + df_res['DK_PROFIT_2'])
    df_res['PROFIT'] = df_res['DK_PROFIT_2']

    df_res['DK_OPEN_VALUE'] = df_res['OPEN_PRICE'] * df_res['QTY'] 
    df_res['DK_BALANCE_OUT'] = df_res['DK_TRANS'].cumsum()
    df_res['DK_BALANCE_IN'] = df_res['DK_BALANCE_OUT'] - df_res['DK_TRANS']
    
    return df_res


def extend_with_grid_details(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Extends every order in source df with grid details:
        - DK_GRID_ID - unique grid id
        - DK_GRIP_QTY - cumulative grid qty for current order
        - DK_OPEN_VALUE - cumulative grid value for current order
    '''

    df_res = df.copy()

    # Filter only orders without another tansactions such as deposits and withdrawals
    df_res = df_res[df_res['COMMENT'].str.contains(COMMENT_PATTERN_FOR_ORDERS_ONLY, regex=False)]
    df_res = df_res.sort_values(by=['OPEN_DT', 'ORDER_ID'], ascending=True)

    df_res = set_grid_id_by_one_side_start(df_res, 'DK_GRID_ID')
    df_res = set_cum_of_grids(df_res, 'DK_GRID_ID', 'QTY', 'DK_GRID_QTY')
    df_res = set_cum_of_grids(df_res, 'DK_GRID_ID', 'DK_OPEN_VALUE', 'DK_GRID_VALUE')

    df_res = set_worst_grid_price(df_res, 'DK_WORST_PRICE')

    df_res['DK_DRAWDOWN'] = abs(df_res['DK_GRID_QTY'] * (df_res['DK_WORST_PRICE']) - df_res['DK_GRID_VALUE']) 
    df_res['DK_DRAWDOWN_RATIO'] = df_res['DK_DRAWDOWN'] / df_res['DK_BALANCE_IN']

    return df_res


def get_grids(df_orders: pd.DataFrame) -> pd.DataFrame:
    """Return df with only with grids

    Args:
        df_orders (pd.DataFrame): Orders dataframe

    Returns:
        pd.DataFrame: Nets dataframe
    """

    df_grids = df_orders.copy()
    df_grids = df_grids.sort_values(by=['OPEN_DT', 'ORDER_ID'], ascending=True)

    df_grids['DK_GRID_LAST_PRICE'] = df_grids['OPEN_PRICE']
    df_grids['DK_GRID_OPEN_QTY'] = df_grids['QTY']

    df_grids = df_grids.groupby(['DK_GRID_ID']).agg({
        'OPEN_DT': 'min',
        'CLOSE_DT': 'max',
        'ORDER_ID': 'count',
        'PROFIT': 'sum',
        'DK_GRID_OPEN_QTY': 'min',
        'QTY': 'sum',
        'OPEN_PRICE': 'first',
        'DK_GRID_LAST_PRICE': 'last',
        'DK_OPEN_VALUE': 'sum',
        'DK_WORST_PRICE': 'last',
        'DK_BALANCE_IN': 'min',
    }).reset_index()

    df_grids['DK_GRID_AVG_PRICE'] = df_grids['DK_OPEN_VALUE'] / df_grids['QTY']
    df_grids['DK_DRAWDOWN'] = abs(df_grids['QTY'] * (df_grids['DK_WORST_PRICE']) - df_grids['DK_OPEN_VALUE'])  
    df_grids['DK_EQUITY'] = df_grids['DK_BALANCE_IN'] - df_grids['DK_DRAWDOWN']
    df_grids['DK_DRAWDOWN_RATIO'] = df_grids['DK_DRAWDOWN'] / df_grids['DK_BALANCE_IN']
    df_grids = df_grids.sort_values(by='DK_DRAWDOWN_RATIO', ascending=False)

    def calc_drawdown_for_max_grid_order(r):
        '''
        Calculate for one given grid potential drawdown of max order in grid if price will go worst case every next step
        '''
        kq = QTY_FACTOR_FOR_AVERAGE
        kp = (OPEN_NEW_ORDER_PRICE_DELTA_PIPS - 1) * XAU_PIP_USD
        
        curr_price = r['DK_GRID_LAST_PRICE']
        curr_value = r['DK_OPEN_VALUE']
        curr_qty = r['QTY']
        for i in range(r['ORDER_ID'] + 1, EVE_MAX_ORDER_COUNT + 1):
            curr_price = curr_price + kp
            curr_value = curr_value + (curr_price) * (curr_qty * kq - curr_qty)
            curr_qty = curr_qty * kq

        return abs(curr_value - curr_qty * (curr_price + kp))

    df_grids['DK_DRAWDOWN_20'] = df_grids.apply(calc_drawdown_for_max_grid_order, axis = 1)
    df_grids['DK_DRAWDOWN_EQUITY_20'] = df_grids['DK_BALANCE_IN'] - df_grids['DK_DRAWDOWN_20']
    df_grids['DK_DRAWDOWN_20_RATIO'] = df_grids['DK_DRAWDOWN_20'] / df_grids['DK_BALANCE_IN']

    df_grids['DK_LOT_1000'] = df_grids['DK_GRID_OPEN_QTY'] / (df_grids['DK_BALANCE_IN'] / 1000)
    df_grids = df_grids.sort_values(by='OPEN_DT')

    return df_grids

def get_summary(df_full: pd.DataFrame, df_orders: pd.DataFrame, df_grids: pd.DataFrame) -> pd.DataFrame:
    """ Calc summary info for account based on dataframes of orders and grids

    Args:
        df_orders (pd.DataFrame): Orders dataframe
        df_grids (pd.DataFrame): Grids dataframe

    Returns:
        pd.DataFrame: Summary dataframe
    """

    df_sum = df_full.copy()

    df_sum['DAYS'] = df_sum['OPEN_DT'].dt.date
    df_sum['BALANCE'] = df_sum['DK_TRANS']
    df_sum['HAS_ORDER_PROFIT'] = 0

    df_sum.loc[df_sum['PROFIT'] >= 0, ['HAS_ORDER_PROFIT']] = 1
    df_sum['AVG_ORDER_PROFIT'] = df_sum['PROFIT']
    df_sum.loc[df_sum['PROFIT'] >= 0, ['MAX_ORDER_PROFIT']] = df_sum['PROFIT']
    df_sum.loc[(df_sum['PROFIT'] < 0), ['MAX_ORDER_LOSS']] = df_sum['PROFIT']

    df_sum = df_sum.groupby(lambda x: True).agg({
        'DAYS': pd.Series.nunique,
        'BALANCE': 'sum',
        'DK_DEPOSIT': 'sum',
        'DK_WITHDRAWAL': 'sum',
        'DK_MISC_TRANS': 'sum',
        'PROFIT': 'sum',
        'ORDER_ID': 'count',
        'HAS_ORDER_PROFIT': 'sum',
        'AVG_ORDER_PROFIT': 'mean',
        'MAX_ORDER_PROFIT': 'max',
        'MAX_ORDER_LOSS': 'min',
    })

    df_grids = df_grids.sort_values(by=['OPEN_DT'])

    df_sum['START_DATE'] = df_full['OPEN_DT'].min().date()
    df_sum['FINISH_DATE'] = df_full['OPEN_DT'].max().date()
    df_sum['CAL_DAYS'] = (df_full['OPEN_DT'].max().date() - df_full['OPEN_DT'].min().date()).days
    
    df_sum['PROFIT_PCT'] = df_sum['PROFIT'] / df_sum['BALANCE']
    df_sum['OWN_FUNDS'] = df_sum['DK_DEPOSIT'] + df_sum['DK_WITHDRAWAL']
    df_sum['PROFIT_PER_DAY'] = df_sum['PROFIT'] / df_sum['DAYS']
    df_sum['PROFIT_PER_CAL_DAY'] = df_sum['PROFIT'] / df_sum['CAL_DAYS']
    df_sum['ROA'] = df_sum['PROFIT'] / df_sum['BALANCE']
    df_sum['ROA_DAYS'] = df_sum['BALANCE'] / df_sum['PROFIT_PER_CAL_DAY']
    df_sum['ROI'] = df_sum['PROFIT'] / df_sum['OWN_FUNDS']
    df_sum['ROI_DAYS'] = df_sum['OWN_FUNDS'] / df_sum['PROFIT_PER_CAL_DAY']
    df_sum['WIN_RATE'] = df_sum['HAS_ORDER_PROFIT'] / df_sum['ORDER_ID']
    df_sum['GRID_CNT'] = df_orders['DK_GRID_ID'].nunique()
    df_sum['MIN_LOT_1000'] = df_grids['DK_LOT_1000'].min()
    df_sum['AVG_LOT_1000'] = df_grids['DK_LOT_1000'].mean()
    df_sum['MAX_LOT_1000'] = df_grids['DK_LOT_1000'].max()
    df_sum['LAST_LOT_1000'] = df_grids.iloc[- 1]['DK_LOT_1000']
    df_sum['AVG_GRID_ORDER_CNT'] = df_grids['ORDER_ID'].mean()
    df_sum['MAX_GRID_ORDER_CNT'] = df_grids['ORDER_ID'].max()
    df_sum['AVG_GRID_PROFIT'] = df_grids['PROFIT'].mean()
    df_sum['MAX_GRID_PROFIT'] = df_grids['PROFIT'].max()
    df_sum['AVG_GRID_DRAWDOWN'] = df_grids['DK_DRAWDOWN'].mean()
    df_sum['MAX_GRID_DRAWDOWN'] = df_grids['DK_DRAWDOWN'].max()
    df_sum['AVG_GRID_DRAWDOWN_RATIO'] = df_grids['DK_DRAWDOWN_RATIO'].mean()
    df_sum['MAX_GRID_DRAWDOWN_RATIO'] = df_grids['DK_DRAWDOWN_RATIO'].max()

    return df_sum


def get_chart(df_grids: pd.DataFrame):
    df_plot = df_grids.copy()
    df_plot['DT'] = df_plot['CLOSE_DT'].dt.date
    df_plot['DK_DRAWDOWN_RATIO'] = df_plot['DK_DRAWDOWN_RATIO'] * 100

    df_plot = df_plot.groupby(by=['DT']).agg({
        'ORDER_ID': 'max',
        'DK_DRAWDOWN': 'max',
        'DK_DRAWDOWN_RATIO': 'max',
        'PROFIT': 'sum',
        'DK_BALANCE_IN': 'min',
        'DK_LOT_1000': 'mean',
        }).reset_index()

    fig, ax = plt.subplots(nrows=6, sharex=True, figsize=(16,9), gridspec_kw={'height_ratios': [3, 1, 2, 1, 2, 1]})

    ax[0].plot(df_plot['DT'], df_plot['DK_BALANCE_IN'], 'b-')
    ax[1].bar(df_plot['DT'], df_plot['ORDER_ID'])
    ax[2].bar(df_plot['DT'], df_plot['DK_DRAWDOWN'], color='red')
    ax[3].bar(df_plot['DT'], df_plot['DK_DRAWDOWN_RATIO'], color='red')
    ax[4].bar(df_plot['DT'], df_plot['PROFIT'], color='green')
    ax[5].bar(df_plot['DT'], df_plot['DK_LOT_1000'], color='grey')

    ax[0].set_ylabel('Депозит, $', fontsize=8)
    ax[1].set_ylabel('Max ордеров, шт',  fontsize=8)
    ax[2].set_ylabel('Просадка, $',  fontsize=8)
    ax[3].set_ylabel('Просадка, %',  fontsize=8)
    ax[4].set_ylabel('Прибыль, $',  fontsize=8)
    ax[5].set_ylabel('Лот на $1000',  fontsize=8)


    def round_half_up(n, decimals=0):
        multiplier = 10 ** decimals
        return math.floor(n*multiplier + 0.5) / multiplier

    def get_ndarray_for_ticks(min: float, max: float, cnt: int) -> np.ndarray:
        step = round(round_half_up((max - min) / cnt))
        r_cnt = len(str(step)) - 1
        r = 10**r_cnt
        step = math.ceil(step / r) * r

        return np.arange(min, step * (cnt + 1), step)

    ax[0].set_yticks(get_ndarray_for_ticks(0, df_plot['DK_BALANCE_IN'].max(), 10))  
    ax[1].set_yticks(np.arange(0, round_half_up(df_plot['ORDER_ID'].max()/2)*2+1, 2))  
    ax[2].set_yticks(get_ndarray_for_ticks(0, df_plot['DK_DRAWDOWN'].max(), 6))  
    ax[3].set_yticks(get_ndarray_for_ticks(0, df_plot['DK_DRAWDOWN_RATIO'].max(), 6))  
    ax[4].set_yticks(get_ndarray_for_ticks(0, df_plot['PROFIT'].max(), 10))  
    ax[5].set_yticks(np.arange(0, df_plot['DK_LOT_1000'].max()+0.005, 0.005))

    for ax_i in ax:
        ax_i.grid()
        ax_i.tick_params(axis='both', which='major', labelsize=6)

    plt.xticks(df_plot['DT'], rotation=90, fontsize=6)
    fig.tight_layout()

    return fig