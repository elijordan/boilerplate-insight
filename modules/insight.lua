
local Insight = {}

Insight.functions = {
  filterOutErrorData = {
    id = "filterOutErrorData",
    name = "Filter Out Error Data",
    description = "Filter incoming Signal Data down to only valid signal data " ..
                  "based on the valid (non-error) range of Signal data",
    constants = {
      {
        name = "ValidLowerBound",
        description = "Lower bound of valid Signal data",
        type = "number"
      },
      {
        name = "ValidUpperBound",
        description = "Upper bound of valid Signal data",
        type = "number"
      }
    },
    inlets = {
      {
        primitive_type = "NUMERIC"
      }
    },
    fn = function(request)
      local dataIN = request.data
      local constants = request.args.constants
      dataOUT = {}

      for _, dp in pairs(dataIN) do

        local val = tonumber(dp.value)
        local upper = constants.ValidUpperBound
        local lower = constants.ValidLowerBound

        if (val >= lower) and (val <= upper) then
          table.insert(dataOUT, dp)
        end

      end
      return {dataOUT}
    end
  },
  thresholds = {
    id = "thresholds",
    name = "Min/Max Thresholds",
    description = "Sets threshold bounds at and beyond which an alert state is generated",
    type = "rule",
    constants = {
      {
        name = "Max",
        description = "Maximum (>=)",
        type = "number"
      },
      {
        name = "Min",
        description = "Minimum (<=)",
        type = "number"
      },
      {
        name = "level",
        description = "The alert level if the bounds are breached (either direction)",
        type = "number",
        enum = {"Warning","Critical"},
        default = 1
      }
    },
    inlets = {
      name = "Input Signal",
      description = "Input signal",
      primitive_type = "NUMERIC"
    },
    outlets = {
      data_type = "STATUS"
    },
    fn = function(request)
      -- Define enum (alert level) mapping
      local levels = {Warning = 2, Critical = 3}

      -- Datapoint is first & only item in data array
      local dp = request.data[1]
      local constants = request.args.constants
      local data_out = {}

      -- Copy relevant content from ingest object
      data_out.ts = dp.ts
      data_out.gts = dp.gts
      data_out.origin = dp.origin
      data_out.generated = dp.generated
      data_out.ttl = dp.ttl
      data_out.value = { value = dp.value }
      data_out.tags = {}

      if dp.value  >= constants.Max then
        data_out.value.level = levels[constants.level]
        data_out.value.type = "Threshold Rule - signal <TODO> has triggered an Upper " .. contstants.level
      elseif dp.value <= constants.Min then
        data_out.value.level = levels[constants.level]
        data_out.value.type = "Threshold Rule - signal <TODO> has triggered a Lower " .. contstants.level
      else
        data_out.value.level = 0
        data_out.value.type = "Threshold Rule - signal <TODO> has returned to be within configured threshold bounds"
      end

      -- Retrieve previous level
      local previous_state = Keystore.get({key = data_out.generated}).value

      -- If the state has not changed, do nothing, else, set new state and push
      if tonumber(previous_state) == tonumber(data_out.value.level) then
        return {{}}
      else
        Keystore.set({key = data_out.generated, value = data_out.value.level})
        return {{data_out}}
      end
    end
  }
}

function Insight.info()
  info = {
    name = "Boilerplate Custom Insights",
    description = "This is a collection of boilerplate custom ExoSense™ Insights",
    group_id_required = false,
    wants_lifecycle_events = false
  }
  return info
end

function Insight.listInsights(request)
  log.warn("List Request: " .. to_json(request))
  local insights = {}
  if request.group_id ~= "" then
    groups = require('groups')
    if groups[request.group_id] then
      for k,v in pairs(groups[request.group_id]) do
        v.fn = nil
        table.insert(insights, v)
      end
    end
  else
    if not Insight.info().group_id_required then
      for k,v in pairs(Insight.functions) do
        v.fn = nil
        v.id = k
        table.insert(insights, v)
      end
    end
  end
  list = {
    total = #insights,
    count = #insights,
    insights = insights
  }

  log.warn("List: " .. to_json(list))
  return list
end

function Insight.infoInsight(request)
  log.warn("Info Request: " .. to_json(request))
  local found = Insight.functions[request.function_id]
  if found == nil then
    return nil, {
      name = "Not Implemented",
      message = "Function \"" .. tostring(request.function_id) .. "\" is not implemented"
    }
  end
  found.id = request.function_id
  found.fn = nil
  log.warn("Info: " .. to_json(found))
  return found
end

function Insight.lifecycle(request)
  log.debug("LIFECYCLE: " .. to_json(request))
  return {}
end

function Insight.process(request)
  log.warn("Process Request: " .. to_json(request))
  local found = Insight.functions[request.args.function_id]
  if found == nil then
    return nil, {
      name = "Not Implemented",
      message = "Function \"" .. tostring(request.args.function_id) .. "\" is not implemented"
    }
  end
  return found.fn(request)
end

return Insight
