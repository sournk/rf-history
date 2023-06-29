# EVE Trading Analyzer by Roboforex.com Account History Export

EVE is grid trading adviser for MT-4. Usually EVE is used at [roboforex.com](https://roboforex.com). 

This project helps you to get analysis of EVE trading result by history export files. You can use Telegram bot [Как дела у EVE?](https://t.me/rfhistoryanalyserbot) to load data and get results of analysis online. Or you can get Jupiter Notebook code from this repo and make analysis on you local machine.

# Implementation Features

## What is the grid?

EVE is close source adviser algorithm. That's why this package makes only assumptions about the implementation of the algorithm for its analysis. In particular, trading grids in this analysis are a sequence of unidirectional orders with an increasing quantity. Although in reality, the robot simultaneously holds several multidirectional grids and the trader simultaneously sees both buy and sell orders, considering them to be one grid.

## How drawdown is calculated?

Max drawdown of grid is a sum of drawdown of all orders in grid. Max drawdown is calculated as possible drawdown when the grid orders could potentially have the worst price equal to `OPEN_PRICE_OF_LAST_GRID_ORDER + (OPEN_NEW_ORDER_PRICE_DELTA_PIPS - 1) * XAU_PIP_USD`. Approximately equal to $1.80 for now.

## Balance

[Roboforex.com](https://roboforex.com) doesn't give any info about balance for every order. The incoming balance for each order is calculated by adding up all previous transactions values. **That's why is so important to give full deposit history for analysis, even if only the short last period is interesting.**

## Worst Price Case Model

We have a model of worst market price movement for every grid. It assumes that the grid did not close, but continued to open orders until the 20th, in conditions of price movement against us in the most unfavorable scenario. The model shows what kind of deposit deficit could occur in this case, which in total is equal to the potential top-up.
