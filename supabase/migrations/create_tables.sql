-- Create cattle_slaughter table (main table for FIS Species data)
CREATE TABLE IF NOT EXISTS cattle_slaughter (
    id BIGSERIAL PRIMARY KEY,
    office_name TEXT,
    office_code TEXT,
    office_city TEXT,
    office_state TEXT,
    description TEXT,
    class TEXT,
    slaughter_date DATE,
    volume NUMERIC,
    unit TEXT,
    type TEXT,
    region TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create meat_production table
CREATE TABLE IF NOT EXISTS meat_production (
    id BIGSERIAL PRIMARY KEY,
    office_name TEXT,
    office_code TEXT,
    office_city TEXT,
    office_state TEXT,
    description TEXT,
    class TEXT,
    slaughter_date DATE,
    volume NUMERIC,
    unit TEXT,
    type TEXT,
    region TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create head_percent table
CREATE TABLE IF NOT EXISTS head_percent (
    id BIGSERIAL PRIMARY KEY,
    office_name TEXT,
    office_code TEXT,
    office_city TEXT,
    office_state TEXT,
    description TEXT,
    class TEXT,
    slaughter_date DATE,
    volume NUMERIC,
    unit TEXT,
    type TEXT,
    region TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create region_data table
CREATE TABLE IF NOT EXISTS region_data (
    id BIGSERIAL PRIMARY KEY,
    office_name TEXT,
    office_code TEXT,
    office_city TEXT,
    office_state TEXT,
    description TEXT,
    class TEXT,
    slaughter_date DATE,
    volume NUMERIC,
    unit TEXT,
    type TEXT,
    region TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_cattle_slaughter_date ON cattle_slaughter(slaughter_date);
CREATE INDEX IF NOT EXISTS idx_meat_production_date ON meat_production(slaughter_date);
CREATE INDEX IF NOT EXISTS idx_head_percent_date ON head_percent(slaughter_date);
CREATE INDEX IF NOT EXISTS idx_region_data_date ON region_data(slaughter_date);

-- Add row level security (RLS) policies
ALTER TABLE cattle_slaughter ENABLE ROW LEVEL SECURITY;
ALTER TABLE meat_production ENABLE ROW LEVEL SECURITY;
ALTER TABLE head_percent ENABLE ROW LEVEL SECURITY;
ALTER TABLE region_data ENABLE ROW LEVEL SECURITY;

-- Create policies to allow read access for authenticated users
CREATE POLICY "Enable read access for authenticated users" ON cattle_slaughter
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "Enable read access for authenticated users" ON meat_production
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "Enable read access for authenticated users" ON head_percent
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "Enable read access for authenticated users" ON region_data
    FOR SELECT TO authenticated USING (true);

-- Create policies to allow insert access for authenticated users
CREATE POLICY "Enable insert access for authenticated users" ON cattle_slaughter
    FOR INSERT TO authenticated WITH CHECK (true);

CREATE POLICY "Enable insert access for authenticated users" ON meat_production
    FOR INSERT TO authenticated WITH CHECK (true);

CREATE POLICY "Enable insert access for authenticated users" ON head_percent
    FOR INSERT TO authenticated WITH CHECK (true);

CREATE POLICY "Enable insert access for authenticated users" ON region_data
    FOR INSERT TO authenticated WITH CHECK (true);

-- Create triggers to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_cattle_slaughter_updated_at
    BEFORE UPDATE ON cattle_slaughter
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_meat_production_updated_at
    BEFORE UPDATE ON meat_production
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_head_percent_updated_at
    BEFORE UPDATE ON head_percent
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_region_data_updated_at
    BEFORE UPDATE ON region_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
