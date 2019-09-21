-- Copyright 1999 Kevin Closson

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--    http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE FUNCTION pgio_get_random_number(BIGINT, BIGINT) RETURNS BIGINT AS $$
DECLARE
    v_low  ALIAS FOR $1;
    v_high ALIAS FOR $2;
BEGIN
    RETURN trunc(random() * (v_high - v_low) + v_low);
END;
$$ LANGUAGE 'plpgsql' STRICT;

