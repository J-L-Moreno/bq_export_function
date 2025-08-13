function countryGroup(countryCodeField) {
  return `case
            when lower(${countryCodeField}) like '%us%' then 'US'
            when lower(${countryCodeField}) like '%india%' then 'India'
            when lower(${countryCodeField}) like '%eu%' then 'EU'
            when lower(${countryCodeField}) like '%asia pacific%' then 'APAC'
            else 'Other'
            end`;
}

module.exports = {
  countryGroup
};