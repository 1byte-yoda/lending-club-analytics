
class Config():
    def __init__(self, env: str):
        self.lending_analytics_dl_bronze_path = f"/mnt/{env}/lendingclubanalyticsdl/bronze"

        self.lending_analytics_dl_silver_path = f"/mnt/{env}/lendingclubanalyticsdl/silver"

        self.lending_analytics_dl_gold_path = f"/mnt/{env}/lendingclubanalyticsdl/gold"
