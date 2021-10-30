import pyodbc
import csv


def csv_holdings_to_list(file_path):
    csv_holdings_rows = []
    opened_file = open(file_path)
    read_file = csv.reader(opened_file)
    next(read_file)
    for holding_row in read_file:
        csv_holdings_rows.append(holding_row)
    opened_file.close()
    return csv_holdings_rows


def daily_return(previous_price, new_price, decimals):
    net_change = new_price - previous_price
    daily_rturn = round(net_change / previous_price * 100, decimals)
    return daily_rturn


def price_in_usd(local_price, exchange_rate):
    return local_price / exchange_rate


def reconcile_holdings(cursor, custodian_holdings):
    iteration_count = 1
    total_holdings_exposure = 0.00
    total_exposure_format = "{0:,.2f}"
    for custodian_holding in custodian_holdings:
        custodian_ticker_name = ''.join(custodian_holding[0])
        custodian_qty = int(custodian_holding[1])
        custodian_local_price = float(custodian_holding[2])
        custodian_exchange_rate = float(custodian_holding[3])
        sql_query = "Exec dbo.Stock_Holdings_Data @Ticker = '" + custodian_ticker_name + "';"
        my_holding = cursor.execute(sql_query).fetchone()
        if my_holding is not None:
            my_holding_qty = my_holding[0]
            if custodian_qty == my_holding_qty:
                my_holding_price = float(my_holding[1])
                monetary_decimal = int(my_holding[3])
                custodian_usd_price = price_in_usd(custodian_local_price, custodian_exchange_rate)
                net_change = custodian_usd_price - my_holding_price
                if net_change != 0:
                    print(custodian_ticker_name, 'Local price', str(custodian_local_price) + ',', 'Exchange rate',
                          str(custodian_exchange_rate) + ',', 'USD price', str(custodian_usd_price) + ',',
                          'Previous price', str(my_holding_price) + ',', 'Net Chg is', str(round(net_change, monetary_decimal)))
                    update_price = "Exec dbo.Update_Holdings_Price @Price = " + str(custodian_usd_price) + ", " + \
                                   "@Ticker = '" + custodian_ticker_name + "'"
                    cursor.execute(update_price)
                    if iteration_count % 5 == 0:
                        cursor.commit()
                else:
                    print('No Gain or Loss for ' + custodian_ticker_name)
                return_decimal = int(my_holding[2])
                print("Percentage return for " + custodian_ticker_name + " is ",
                      str(daily_return(my_holding_price, custodian_usd_price, return_decimal)) + "%")
                ticker_exposure_format = "{0:,." + str(monetary_decimal) + "f}"
                holding_exposure = custodian_usd_price * my_holding_qty
                print(custodian_ticker_name + ' Exposure = $' + ticker_exposure_format.format(holding_exposure))
                total_holdings_exposure += holding_exposure
            else:
                print('Ticker ' + custodian_ticker_name + ' Custodian qty', custodian_qty, ', My Qty is',
                      my_holding_qty)
        else:
            print("Ticker name " + custodian_ticker_name + " doesn't exist in Database.")
        iteration_count += 1
    print('Total Holdings exposure = $' + total_exposure_format.format(round(total_holdings_exposure, 2)))


holdings_csv_path = r'input/Holdings.csv'
connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                            'Server=DESKTOP-IQBNFKO;'
                            'Database=Keith_Test;'
                            'Trusted_Connection=yes')

reconcile_holdings(connection, csv_holdings_to_list(holdings_csv_path))
connection.commit()
connection.close()
