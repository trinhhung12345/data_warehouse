-- --- 1. DimDate ---
DROP TABLE IF EXISTS DimDate CASCADE;
CREATE TABLE DimDate (
                         DateKey INTEGER PRIMARY KEY, -- Định dạng YYYYMMDD (VD: 20240101)
                         FullDate DATE NOT NULL,
                         CalendarYear INTEGER,
                         CalendarQuarter VARCHAR(10),
                         MonthNumberOfYear INTEGER,
                         MonthName VARCHAR(20),
                         CalendarYearMonth VARCHAR(10),
                         DayNumberOfMonth INTEGER,
                         DayNumberOfYear INTEGER,
                         DayNameOfWeek VARCHAR(20),
                         WeekNumberOfYear INTEGER,
                         IsWeekDay BOOLEAN,
                         IsHoliday BOOLEAN,
                         HolidayName VARCHAR(50)
);

-- --- 2. DimLocation ---
DROP TABLE IF EXISTS DimLocation CASCADE;
CREATE TABLE DimLocation (
                             LocationKey SERIAL PRIMARY KEY, -- Surrogate Key
                             LocationID INTEGER NOT NULL,    -- Business Key (Gốc từ TLC)
                             ZoneName VARCHAR(255),
                             Borough VARCHAR(50),
                             ServiceZone VARCHAR(50),
                             ZoneGeometry GEOMETRY(GEOMETRY, 4326), -- Yêu cầu extension PostGIS
                             StartDate TIMESTAMP, -- SCD Type 2
                             EndDate TIMESTAMP,   -- SCD Type 2
                             IsCurrent BOOLEAN DEFAULT TRUE
);

-- --- 3. DimDriver ---
DROP TABLE IF EXISTS DimDriver CASCADE;
CREATE TABLE DimDriver (
                           DriverKey SERIAL PRIMARY KEY, -- Surrogate Key
                           DriverID INTEGER NOT NULL,    -- Business Key (Gốc từ Ops)
                           DriverName VARCHAR(255),
                           LicenseNumber VARCHAR(50),
                           DriverStatus VARCHAR(20),
                           StartDate TIMESTAMP, -- SCD Type 2
                           EndDate TIMESTAMP,   -- SCD Type 2
                           IsCurrent BOOLEAN DEFAULT TRUE
);

-- --- 4. DimCustomer ---
DROP TABLE IF EXISTS DimCustomer CASCADE;
CREATE TABLE DimCustomer (
                             CustomerKey SERIAL PRIMARY KEY, -- Surrogate Key
                             CustomerID INTEGER NOT NULL,    -- Business Key (Gốc từ CRM)
                             CustomerName VARCHAR(255),
                             PhoneNumber VARCHAR(20),
                             Email VARCHAR(255),
                             CustomerSegment VARCHAR(50),
                             RegistrationDate DATE,
                             RegistrationYear INTEGER,
                             RegistrationQuarter VARCHAR(10),
                             CustomerAgeOnPlatform INTEGER, -- Tính toán (Năm hiện tại - Năm đăng ký)
                             StartDate TIMESTAMP, -- SCD Type 2
                             EndDate TIMESTAMP,   -- SCD Type 2
                             IsCurrent BOOLEAN DEFAULT TRUE
);

-- --- 5. DimVehicle ---
DROP TABLE IF EXISTS DimVehicle CASCADE;
CREATE TABLE DimVehicle (
                            VehicleKey SERIAL PRIMARY KEY, -- Surrogate Key
                            VehicleID INTEGER NOT NULL,    -- Business Key (Gốc từ Ops)
                            VehicleMakeModel VARCHAR(100),
                            VehicleColor VARCHAR(50),
                            VehicleCapacity INTEGER,
                            RegistrationYear INTEGER,
                            StartDate TIMESTAMP, -- SCD Type 2
                            EndDate TIMESTAMP,   -- SCD Type 2
                            IsCurrent BOOLEAN DEFAULT TRUE
);

-- --- 6. DimPromotion ---
DROP TABLE IF EXISTS DimPromotion CASCADE;
CREATE TABLE DimPromotion (
                              PromotionKey SERIAL PRIMARY KEY, -- Surrogate Key
                              PromotionID INTEGER NOT NULL,    -- Business Key (Gốc từ CRM)
                              PromotionCode VARCHAR(50),
                              PromotionName VARCHAR(255),
                              Description TEXT,
                              DiscountType VARCHAR(20),
                              DiscountValue DECIMAL(10, 2),
                              Campaign VARCHAR(100),
                              StartDate TIMESTAMP,
                              EndDate TIMESTAMP,
                              DurationInDays INTEGER,
                              PromotionStatus VARCHAR(20)
);

-- --- 7. FactTrip ---
DROP TABLE IF EXISTS FactTrip CASCADE;
CREATE TABLE FactTrip (
                          TripKey BIGSERIAL PRIMARY KEY,

    -- Foreign Keys (Dimensions)
                          DateKey INTEGER NOT NULL,            -- Liên kết DimDate (Ngày Pickup)
                          PickupLocationKey INTEGER NOT NULL,  -- Liên kết DimLocation
                          DropoffLocationKey INTEGER NOT NULL, -- Liên kết DimLocation
                          DriverKey INTEGER NOT NULL,          -- Liên kết DimDriver
                          VehicleKey INTEGER NOT NULL,         -- Liên kết DimVehicle
                          CustomerKey INTEGER NOT NULL,        -- Liên kết DimCustomer
                          PromotionKey INTEGER NOT NULL,       -- Liên kết DimPromotion (Dùng key giả định nếu không có KM)

    -- Measures (Chỉ số đo lường tài chính & Vận hành)
                          FareAmount DECIMAL(10, 2),
                          Extra DECIMAL(10, 2),
                          MtaTax DECIMAL(10, 2),
                          TipAmount DECIMAL(10, 2),
                          TollsAmount DECIMAL(10, 2),
                          ImprovementSurcharge DECIMAL(10, 2),
                          CongestionSurcharge DECIMAL(10, 2),
                          TotalAmount DECIMAL(10, 2),

                          TripDistance DECIMAL(10, 2),
                          TripDuration INTEGER, -- Tính bằng giây hoặc phút

    -- Measures (Chỉ số chất lượng - Aggregate từ bảng Feedback)
                          AverageRating DECIMAL(3, 2),
                          AcceptanceRate DECIMAL(5, 2),

    -- Ràng buộc Khóa Ngoại
                          CONSTRAINT fk_fact_date FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
                          CONSTRAINT fk_fact_pickup_loc FOREIGN KEY (PickupLocationKey) REFERENCES DimLocation(LocationKey),
                          CONSTRAINT fk_fact_dropoff_loc FOREIGN KEY (DropoffLocationKey) REFERENCES DimLocation(LocationKey),
                          CONSTRAINT fk_fact_driver FOREIGN KEY (DriverKey) REFERENCES DimDriver(DriverKey),
                          CONSTRAINT fk_fact_vehicle FOREIGN KEY (VehicleKey) REFERENCES DimVehicle(VehicleKey),
                          CONSTRAINT fk_fact_customer FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
                          CONSTRAINT fk_fact_promotion FOREIGN KEY (PromotionKey) REFERENCES DimPromotion(PromotionKey)
);