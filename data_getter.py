import requests
import yfinance as yf
#import pandas as pd
import time
from datetime import datetime, timedelta

# GDELT GKG THEMES


# MARKET SYMBOLS

BATCH_SYMBOLS = [
    # Global ETFs for country coverage
    # "EEM",   # Emerging Markets (covers many countries)
    # "VWO",   # FTSE Emerging Markets
    # "IEMG",  # Core MSCI Emerging Markets

    # Regional ETFs
    "AFK",   # Africa ETF
    "PAF",      # Pan Africa ETF
    "GAF",   # Middle East & Africa
    "EZA",   # South Africa
    "TUR",   # Turkey
    "EGPT",  # Egypt
    "NGE"   # Nigeria
    # TODO: ADD 50+ country ETFs
]

COUNTRY_ETFS = [
    # Developed Markets
    "EWU",  # UK
    "EWG",  # Germany
    "EWQ",  # France
    "EWJ",  # Japan
    "EWC",  # Canada

    # Emerging Markets
    "EWZ",  # Brazil
    "EWW",  # Mexico
    "EPU",  # Peru
    "ECH",  # Chile
    "EPOL", # Poland
    "ERUS", # Russia (if available)
    "EWY",  # South Korea
    "EWT",  # Taiwan
    "EWH",  # Hong Kong
    "INDA", # India

    # Frontier Markets
    # "FM",   # Frontier Markets ETF
    # "FRN",  # Frontier Markets ETF alternative

    # Specific challenging countries
    "ARGT", # Argentina
    "TUR",  # Turkey
    "THD",  # Thailand
    "IDX",  # Indonesia
    "EPHE", # Philippines
    "EWM",  # Malaysia
    "EWS",  # Singapore
]

WEAPONS_DEFENSE = [ # tested
    {"name": "LMT", "position": [39.047915, -77.120115]},   # USA - Lockheed Martin North Bethesda MaryLand 
    {"name": "RTX", "position": [42.396137, -71.261743]},   # USA - Raytheon Waltham Massachussets
    {"name": "BA", "position": [38.856571, -77.063073]},    # USA - Boeing (defense division)
    {"name": "NOC", "position": [38.907401, -77.212519]},   # USA - Northrop Grumman
    {"name": "GD", "position": [38.954878, -77.376298]},    # USA - General Dynamics
    {"name": "SAF.PA", "position": [48.854756, 2.319250]},# France - Safran
    {"name": "AIR.PA", "position": [43.599679, 1.431466]},# France - Airbus
    {"name": "BAESY", "position": [51.490570, -0.141467]}, # UK - BAE Systems London
    {"name": "RHHVF", "position": [51.233609, 6.788247]}, # Germany - Rheinmetall
    {"name": "7011.T", "position": [32.753850, 129.875522]},# Japan - Mitsubishi Heavy Industries marked in Nagasaki
    {"name": "HAL.NS", "position": [12.985674, 77.592147]},# India - Hindustan Aeronautics
    {"name": "302132.SZ", "position": [30.655040, 104.071639]},  # China - AVIC Aviation Industry Corp (via ETF)
]

OIL_ENERGY = [ # tested
    # Global majors
    {"name": "XOM", "position": [30.068842, -95.417203]},   # USA - Exxon - Spring Texas
    # {"name": "CVX", "position": [29.650824, -95.299394]},   # USA - Chevron
    {"name": "BP", "position": [51.480308, -0.101642]},    # UK - BP
    # "SHEL",  # UK/Netherlands - Shell
    {"name": "TTE", "position": [48.891025, 2.243065]},   # France - TotalEnergies

    # National oil companies (where traded)
    {"name": "2222.SR", "position": [26.207381, 50.012410]}, # Saudi Arabia - Saudi Aramco
    {"name": "PBR", "position": [-22.878367, -43.237309]},     # Brazil - Petrobras
    {"name": "PKX", "position": [37.581088, 127.026997]},     # South Korea - POSCO
    {"name": "CEO", "position": [39.92369, 116.42637]},     # China - CNOOC
    # DELISTED "OGDC.PK", # Pakistan - Oil & Gas Dev
    {"name": "ENI.MI", "position": [45.458280, 9.146134]},  # Italy - ENI
    {"name": "EQNR", "position": [58.969383, 5.728082]},    # Norway - Equinor
    {"name": "YPF", "position": [-34.652731, -58.416152]},     # Argentina - YPF
]

NATIONAL_BANKS = [ # TESTED
    # USA
    "JPM", "BAC", "C", "WFC",

    # Europe
    "HSBA.L",   # UK - HSBC
    "SAN.MC",   # Spain - Santander
    "GLE.PA",   # France - Société Générale
    "UBSG.SW",  # Switzerland - UBS
    "INGA.AS",  # Netherlands - ING

    # Asia
    "8306.T",   # Japan - Mitsubishi UFJ
    "3968.HK",  # China - Bank of China
    "HDFCBANK.NS", # India - HDFC Bank
    "KB",       # South Korea - KB Financial

    # Emerging Markets
    "ITUB",     # Brazil - Itaú Unibanco
    "BBAR",     # Argentina - Banco BBVA Argentina
    "SBK.JO",   # South Africa - Standard Bank
    "QNBK.QA",  # Qatar - Qatar National Bank
    # DELISTED "NCB.AB",   # Saudi Arabia - National Commercial Bank
]

AFRICA_COVERAGE = [ # TESTED
    # South Africa (most developed market)
    "EZA",      # South Africa ETF
    "NPN.JO",   # Naspers
    "ANG.JO",   # AngloGold
    "SBK.JO",   # Standard Bank

    # Nigeria
    # DELISTED "NGE",      # Nigeria ETF
    # DELISTED "DANGCEM.NG", # Dangote Cement

    # Egypt
    # DELISTED "EGPT",     # Egypt ETF
    # DELISTED "COMI.CA",  # Commercial International Bank Egypt
]

# RUSSIA = [ # DELISTED
#     "UAC.ME", # United Aircraft Corporation
#     "KTRV.ME", # Tactical Missiles Corporation
#     "KALASH.ME" # Kalashnikov Concern
# "MOEX-PIKK", # homebuilding
# "MOEX-MAGN", # magnitogorsk steeel
# "MOEX-YDEX", # Yandex
# "MOEX-IRKT", # Yakovlev
# "MOEX-SIBN", # Gazprom Neft
# # "MOEX-SBER" # Sberbank
#     "GAZP.ME", # Gazprom
#     "DZRDP.ME"
# ]

WEST_ASIA = [ # tested
    # ETFs for coverage
    # DELISTED"GULF",     # Middle East Dividend ETF
    # DELISTED "MES",      # UAE ETF

    # Individual markets (where available)
    "QNBK.QA",  # Qatar National Bank
    # DELISTED "STC.AB",   # Saudi Telecom
    # "EMIRATES.AD", # Emirates NBD (UAE) # DELISTED
    "2318.HK",  # Ping An (China insurance, but trades in region)

    # Israel
    "EIS",      # Israel ETF
    "TEVA",     # Teva Pharmaceutical (Israel)
    "ESLT", # Elbit Systems Ltd
    # "IAI.TA", # Israel Aerospace Industries

    # "GFH.BH", # Gulf Financial - Bahrain

    "BALSU.IS", # Food - Istanbul
    "SAZEW.KA", # Car manufacturer - Lahore Pakistan
    "MLCF.KA", # Maple Leaf Cement Lahore Pakistan 
    ]

CENTRAL_ASIA = [
    # Iran
    # "PGPIC", # Persian Gulf Petrochemical Industries Company	Petrochemicals
    # "IKCO",  # Iran Khodro Company	Automotive
    # "SADA",  # Saipa Group	Automotive
    "MAMA",  # Mobarakeh Steel Company	Steel Production
    # "NICICO",    # National Iranian Copper Industries	Mining
    # "IORC"  # Isfahan Oil Refining Company	Oil Refining
]

EAST_ASIA = [ # TESTED
    # Malaysia
    "MILDEF.ST", # Mildef International Berhad - Rugged ßIT for Defense - Malaysia
    "2330.TW", # Taiwan SemiConductors
    "0700.HK", # TenCent
    "9988.HK", # AliBaba
    "1810.HK", # XiaoMi
    "0939.HK", # Construction Bank China
    "0425.HK" # Automotive China

 ]

SOUTH_ASIA = [ # TESTED
    "HDFCBANK.NS", # HDFC Bank Mumbai
    "RELIANCE.NS" # Reliance Industries petrochemical Mumbai
]



def get_two_hour_change_alt(ticker_symbol):
    """Alternative method using Ticker object"""

    ticker = yf.Ticker(ticker_symbol)

    # Get intraday data for today
    # '1h' = 1-hour intervals, '30m' = 30-minute, '15m' = 15-minute
    intraday_data = ticker.history(period='', interval='1h')

    if intraday_data.empty:
        return None, None, None, None

    # Get current price
    current_price = intraday_data['Close'].iloc[-1]

    # Find price 2 hours ago (if we have enough data points)
    if len(intraday_data) >= 3:  # Need at least 3 hours of data
        price_two_hours_ago = intraday_data['Close'].iloc[-3]
    elif len(intraday_data) >= 2:  # If only 2 hours, use 1 hour ago
        price_two_hours_ago = intraday_data['Close'].iloc[-2]
    else:
        price_two_hours_ago = intraday_data['Close'].iloc[0]

    # Calculate changes
    price_change = current_price - price_two_hours_ago
    percent_change = (price_change / price_two_hours_ago) * 100

    return current_price, price_two_hours_ago, price_change, percent_change

if __name__ == "__main__":
    curprc, twohrsprc, pricchng, percchng = get_two_hour_change_alt("OGDC.L")
    print("ENI.MI", curprc, twohrsprc, pricchng, percchng)
    # for tickerName in WEAPONS_DEFENSE:
    #     symbol = tickerName["name"]
    #     curprc, twohrsprc, pricchng, percchng = get_two_hour_change_alt(symbol)
    #     print (symbol, curprc, twohrsprc, pricchng, percchng)