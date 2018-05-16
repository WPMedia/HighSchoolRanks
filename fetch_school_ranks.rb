#!/usr/local/bin/ruby
require 'csv'
require 'json'
require 'faraday'


def fetch()
  conn = Faraday.new(:url => 'https://sub.washingtonpost.com/api/v1/elasticSearch/5a21d3dd38a2d80abf361ce6/subs.json?page=1&size=3500', 
    :headers => {"subaccesskey" =>"e867e2a552a8fc0babeb096c8d1bb054227c67aa51bb0b29",
                 "subtoken" => "49d3349905e7ccc43fbfc9ce618731b10a5bc547bae85e33",
                 "Content-type" => "application/json"}) 
  resp = conn.post do |req|
    # puts req.body
  end

  return JSON.parse(resp.body)
end

def calculate_index(data)
  items = []
  data["Submissions"].each do |submission|
    form_data = submission["formData"]
    idx = 0.0
    unless form_data.nil?
      ap_tests_given = form_data["total_ap_tests_1"] || 0
      ib_exams_given = form_data["total_ib_or_aice_exams_2"] || 0
      overlap = form_data["apib_overlap_total_3"] || 0
      tests = ((ap_tests_given + ib_exams_given) - overlap).to_f
      seniors_graduated = form_data["seniors_graduated_0"].to_f
      
      idx = tests / seniors_graduated if seniors_graduated > 0 && tests > 0
    end
    out = form_data
    out["index"] = idx
    items << out
  end

  return items
end

def calculate_rank(data)
  items = []
  begin  
    data.sort {|a,b| b["index"] <=> a["index"]}
      .each_with_index do |submission, idx|
        out = Hash[submission]
        out["rank"] = idx + 1
        items << out
      end
  rescue ArgumentError => e
    puts e.message
  end

  return items
end

def json_to_csv(items, outfile)
  headers = items[0].keys
  CSV.open(outfile,"wt", headers: true) do |csv|
  
    csv << headers
    
    items.each do |s|
      csv << s.values_at(*headers)
    end
  end
end

def normalize(ranks)
  ranks = json_safe_ints(ranks)
  ranks = strip_keys(ranks)
  ranks = parse_address(ranks)

  return ranks
end

def json_safe_ints(ranks)
  toints = [
    "index",
    "rank",
    "seniors_graduated_0",
    "total_ap_tests_1",
    "total_ib_or_aice_exams_2",
    "apib_overlap_total_3",
    "percent_freereduced_lunch_4",
    "equity_in_excellence_percentage_5",
    "percent_ap_scores_3_or_ib_scores_4_6",
    "2016_sat_average_score_7",
    "2016_act_average_score_8",
    "caucasianwhite_17",
    "hispaniclatino_18",
    "africanamericanblack_19",
    "asianpacific_islander_20",
    "native_american_21",
    "multiethnic_22",
    "other_23",
    "number_of_ap_courses_offered_24",
    "number_of_ib_courses_offered_25",
    "number_of_aice_courses_offered_26",
    "enrollment_29",
    "4year_graduation_rate_31",
    "percent_in_4year_colleges_32",
    "percent_taking_satact_34",
    "principals_years_of_experience_35",
    "studentteacher_ratio_36",
    "age_of_institution_43"
  ]

  items = []
  ranks.each do |line|
    e = Hash[line.to_h]
    toints.each do |k|
      e[k] = (e[k]).to_f if e.has_key?(k)
    end

    items << e
  end
  
  return items
end

def strip_keys(ranks)
  items = []
  ranks.each do |school|
    s = {}
    school.each do |key, value|
      # Grab substring before any trailing _##
      new_key = key.split(/_\d+/)[0]
      s[new_key] = value
    end

    items << s
  end

  return items
end

def parse_address(ranks)
  ranks.each do |school|
    if school["school_address"]
      address_line = school["school_address"]
      address_parts = address_line.split(",").map(&:strip)
      school["school_address_city"] = address_parts[2] || ''
      school["school_address_state"] = address_parts[3] || ''
    end
  end

  return ranks
end



data = fetch()
idx = calculate_index(data)
rank = calculate_rank(idx)
rank = normalize(rank)
File.open("./ranks.json","wt") {|f| f.write(JSON.dump(rank))}

json_to_csv(rank, "./ranks.csv")
