local Insight = require('insight').functions

-- Edit this table to contol which functions are returned for which provided group ids (keys)
local Groups = {
  a = {
    Insight.filterToErrorData,
  },
  r = {
    Insight.thresholds
  }
}

return Groups
