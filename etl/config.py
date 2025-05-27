class DatabaseConfig:
    """Database connection configuration"""
    def __init__(self):
        self.server = "localhost,1433"
        self.database = "HealthcareWaitTimes"
        self.username = "sa"
        self.password = "YourStrong@Password123"
        self.driver = "{ODBC Driver 18 for SQL Server}"
        
    def get_connection_string(self) -> str:
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
        )