# import requests
# import pandas as pd

# # -------------------------------
# # LIVE USD → INR (SAFE VERSION)
# # -------------------------------
# def get_live_usd_inr():
#     url = "https://api.exchangerate.host/latest?base=USD&symbols=INR"
#     r = requests.get(url, timeout=10).json()

#     if "rates" not in r:
#         print("API ERROR:", r)
#         return None

#     return r["rates"]["INR"]

# # -------------------------------
# # HISTORICAL USD → INR
# # -------------------------------
# def get_historical_usd_inr(start_date, end_date):
#     url = (
#         f"https://api.exchangerate.host/timeseries"
#         f"?start_date={start_date}&end_date={end_date}"
#         f"&base=USD&symbols=INR"
#     )
#     r = requests.get(url, timeout=10).json()

#     if "rates" not in r:
#         print("API ERROR:", r)
#         return pd.DataFrame()

#     data = [
#         {"date": d, "usd_inr": r["rates"][d]["INR"]}
#         for d in sorted(r["rates"].keys())
#         if r["rates"][d] and r["rates"][d]["INR"] is not None
#     ]

#     return pd.DataFrame(data)

# # -------------------------------
# # MAIN
# # -------------------------------
# if __name__ == "__main__":

#     # LIVE RATE
#     live_rate = get_live_usd_inr()
#     print("Live USD → INR:", live_rate)

#     # HISTORICAL EXAMPLE
#     df = get_historical_usd_inr("2022-01-01", "2022-12-31")
#     print(df.head())

#     df.to_csv("usd_inr_2022.csv", index=False)
#     print("Saved usd_inr_2022.csv")


import yfinance as yf
t=yf.Ticker("AAPL")
print(t.history(start="2024-12-24", end="2024-12-25"))


# # e4ddb799366a45f4a9ff5d27b3e53fab