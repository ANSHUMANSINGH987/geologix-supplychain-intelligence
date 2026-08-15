-- ============================================================================
-- Project: GeoLogix Supply Chain Intelligence Engine
-- Layer: Star Schema Dimensional DDL Specification
-- Target RDBMS: PostgreSQL 15+ / Supabase / SQLite
-- ============================================================================

-- 1. Dim_Date
CREATE TABLE IF NOT EXISTS Dim_Date (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    WeekNumber INT NOT NULL,
    DayOfWeek INT NOT NULL,
    IsWeekend BOOLEAN NOT NULL
);

-- 2. Dim_Port
CREATE TABLE IF NOT EXISTS Dim_Port (
    PortKey INT PRIMARY KEY,
    UNLOCODE VARCHAR(10) UNIQUE NOT NULL,
    PortName VARCHAR(100) NOT NULL,
    Country VARCHAR(100) NOT NULL,
    Region VARCHAR(50) NOT NULL,
    Latitude NUMERIC(9,6) NOT NULL,
    Longitude NUMERIC(9,6) NOT NULL,
    BerthCapacity INT NOT NULL,
    AvgTurnaroundHours NUMERIC(5,2) NOT NULL
);

-- 3. Dim_Chokepoint
CREATE TABLE IF NOT EXISTS Dim_Chokepoint (
    ChokepointKey INT PRIMARY KEY,
    ChokepointCode VARCHAR(20) UNIQUE NOT NULL,
    ChokepointName VARCHAR(100) NOT NULL,
    Region VARCHAR(50) NOT NULL,
    BaselineRiskScore INT NOT NULL, -- Scaled 0 (Safe) to 100 (Extreme Threat)
    GeopoliticalZone VARCHAR(100) NOT NULL,
    Latitude NUMERIC(9,6) NOT NULL,
    Longitude NUMERIC(9,6) NOT NULL
);

-- 4. Dim_Vessel
CREATE TABLE IF NOT EXISTS Dim_Vessel (
    VesselKey INT PRIMARY KEY,
    IMONumber VARCHAR(20) UNIQUE NOT NULL,
    VesselName VARCHAR(100) NOT NULL,
    VesselType VARCHAR(50) NOT NULL, 
    CarrierOperator VARCHAR(100) NOT NULL,
    TEUCapacity INT NOT NULL,
    DWT INT NOT NULL,
    DailyFuelBurnTons NUMERIC(6,2) NOT NULL
);

-- 5. Dim_Route
CREATE TABLE IF NOT EXISTS Dim_Route (
    RouteKey INT PRIMARY KEY,
    RouteCode VARCHAR(50) UNIQUE NOT NULL,
    RouteName VARCHAR(150) NOT NULL,
    OriginPortKey INT REFERENCES Dim_Port(PortKey),
    DestPortKey INT REFERENCES Dim_Port(PortKey),
    StandardChokepointKey INT REFERENCES Dim_Chokepoint(ChokepointKey),
    StandardDistanceNM NUMERIC(8,2) NOT NULL,
    DivertedDistanceNM NUMERIC(8,2) NOT NULL,
    StandardTransitDays NUMERIC(5,2) NOT NULL,
    DivertedTransitDays NUMERIC(5,2) NOT NULL
);

-- 6. Fact_Voyage_Disruptions
CREATE TABLE IF NOT EXISTS Fact_Voyage_Disruptions (
    VoyageID VARCHAR(50) PRIMARY KEY,
    DateKey INT REFERENCES Dim_Date(DateKey),
    VesselKey INT REFERENCES Dim_Vessel(VesselKey),
    RouteKey INT REFERENCES Dim_Route(RouteKey),
    OriginPortKey INT REFERENCES Dim_Port(PortKey),
    DestPortKey INT REFERENCES Dim_Port(PortKey),
    ChokepointKey INT REFERENCES Dim_Chokepoint(ChokepointKey),
    IsDiverted BOOLEAN NOT NULL,
    ActualDistanceNM NUMERIC(8,2) NOT NULL,
    StandardTransitDays NUMERIC(5,2) NOT NULL,
    ActualTransitDays NUMERIC(5,2) NOT NULL,
    DelayDays NUMERIC(5,2) NOT NULL,
    FuelConsumedMT NUMERIC(8,2) NOT NULL,
    ExcessFuelMT NUMERIC(8,2) NOT NULL,
    ExcessCO2MetricTons NUMERIC(8,2) NOT NULL, -- 3.114 MT CO2 per MT of Marine Bunker Fuel
    SpotFreightRateUSD NUMERIC(10,2) NOT NULL,
    WarRiskSurchargeUSD NUMERIC(10,2) NOT NULL,
    CargoValueUSD NUMERIC(12,2) NOT NULL,
    HoldingCostImpactUSD NUMERIC(10,2) NOT NULL
);

-- Analytical Performance Indexes
CREATE INDEX IF NOT EXISTS idx_fact_date ON Fact_Voyage_Disruptions(DateKey);
CREATE INDEX IF NOT EXISTS idx_fact_route ON Fact_Voyage_Disruptions(RouteKey);
CREATE INDEX IF NOT EXISTS idx_fact_vessel ON Fact_Voyage_Disruptions(VesselKey);
CREATE INDEX IF NOT EXISTS idx_fact_chokepoint ON Fact_Voyage_Disruptions(ChokepointKey);