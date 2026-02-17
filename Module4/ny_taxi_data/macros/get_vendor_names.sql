 {%macro get_vendor_names(vendorid)-%}


case
        when {{vendorid}} = 1 then 'Creative Mobile Technologies, LLC' 
        when {{vendorid}} = 2 then 'VeriFone Inc.' 
        when {{vendorid}} = 4 then 'Unknown Vendor'
end
{%-endmacro%}