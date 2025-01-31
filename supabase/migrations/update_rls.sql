-- Drop existing policies
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON cattle_slaughter;
DROP POLICY IF EXISTS "Enable insert access for authenticated users" ON cattle_slaughter;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON meat_production;
DROP POLICY IF EXISTS "Enable insert access for authenticated users" ON meat_production;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON head_percent;
DROP POLICY IF EXISTS "Enable insert access for authenticated users" ON head_percent;
DROP POLICY IF EXISTS "Enable read access for authenticated users" ON region_data;
DROP POLICY IF EXISTS "Enable insert access for authenticated users" ON region_data;

-- Create new policies for cattle_slaughter
CREATE POLICY "Enable read access for all users" ON cattle_slaughter
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for service role" ON cattle_slaughter
    FOR INSERT WITH CHECK (auth.role() = 'service_role');

-- Create new policies for meat_production
CREATE POLICY "Enable read access for all users" ON meat_production
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for service role" ON meat_production
    FOR INSERT WITH CHECK (auth.role() = 'service_role');

-- Create new policies for head_percent
CREATE POLICY "Enable read access for all users" ON head_percent
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for service role" ON head_percent
    FOR INSERT WITH CHECK (auth.role() = 'service_role');

-- Create new policies for region_data
CREATE POLICY "Enable read access for all users" ON region_data
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for service role" ON region_data
    FOR INSERT WITH CHECK (auth.role() = 'service_role');
