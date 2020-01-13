# frozen_string_literal: true

require File.expand_path(File.dirname(__FILE__) + "/base.rb")
require 'mongo'

# Import YahooGroups data as exported into MongoDB by:
#   https://github.com/jonbartlett/yahoo-groups-export
#
#   ceate a ".env" file or export each var assigngment...
#
#   CATEGORY_ID=<CATEGORY_ID>
#   MONGODB_HOST=...
#   MONGODB_DB=...
#   

# load 'script/import_scripts/yahoogroup.rb'
# ImportScripts::YahooGroup.new.perform

class ImportScripts::YahooGroup < ImportScripts::Base

  MONGODB_HOST = ENV['MONGODB_HOST'] || '192.168.10.1:27017'
  MONGODB_DB   = ENV['MONGODB_DB'] || 'syncro'
  CATEGORY_ID = ENV['CATEGORY_ID']

  def initialize
    super

    client = Mongo::Client.new([ MONGODB_HOST ], database: MONGODB_DB)
    db = client.database
    Mongo::Logger.logger.level = Logger::FATAL
    puts "connected to db...."

    @collection = client[:posts]

    @user_profile_map = {}

  end

  def execute
    puts "", "Importing from Mongodb...."
    puts "Existing imported:#{lookup.users.length}"
  #  import_users
  #  lookup.fetch_users
    puts "Existing imported:#{lookup.users.length}"
    import_discussions
    puts "", "Done"
  end

  def import_users

    puts '', "Importing users"

    # fetch distinct list of Yahoo "profile" names
    profiles = @collection.aggregate(
                 [
                  { "$group": { "_id": { profile: "$ygData.profile"  } } }
                 ]
            )

    user_id = 0

    create_users(profiles.to_a) do |u|

      user_id = user_id + 1  ### this is just a dumb count

      # fetch last message for profile to pickup latest user info as this may have changed
      user_info = @collection.find("ygData.profile": u["_id"]["profile"]).sort("ygData.msgId": -1).limit(1).to_a[0]

      # NOTE: do not Store MADE UP user_id in profile lookup
      # instead, must use something that is both uniq and stable (immutable)
      # so, we are going to use the profile, BUT, we can't do it until after we are done
      # since we also need the rails id, which we won't have until we are done.
      #@user_profile_map.store(user_info["ygData"]["profile"], user_id)
      ### also, the lookup_container already does this mapping for us using UserCustomFields
      # (yes, name of the field is import_id); so why re-invent the wheel?
      profile     = user_info["ygData"]["profile"]
      name        = user_info["ygData"]["authorName"]
      raw_email   = user_info["ygData"]["from"]
      clean_email = extract_email(raw_email)

      puts "User #{user_id}: #{profile}, #{raw_email} => #{clean_email}"

      user = {
        id:       profile,  # do NOT generate this; it is stored in the User record (custom field) as import_id
        username: profile,
        name:     name,
        email:    clean_email, # mandatory
        created_at: Time.now
      }
      clean_email ?  user : nil
    end

    puts "#{user_id} users created"

  end

  def extract_email(s)
    m = s.match(/(?:&lt|<);(.*?)(?:&gt|>)/)
    m ? m[1] : s
  end

  def user_id_from_profile(profile)
    lookup.users[profile] || -1
  end

  def import_discussions
    puts "", "Importing discussions"

    topics_count = 0
    posts_count = 0

    topics = @collection.aggregate(
                 [
                  { "$group": { "_id": { topicId: "$ygData.topicId"  } } }
                 ]
    ).to_a

    # for each distinct topicId found
    topics.each_with_index do |t, tidx|

      # create "topic" post first.
      # fetch topic document
      topic_post = @collection.find("ygData.msgId": t["_id"]["topicId"]).to_a[0]
      next if topic_post.nil?

      puts "Topic: #{tidx + 1} / #{topics.count()}  (#{sprintf('%.2f', ((tidx + 1).to_f / topics.count().to_f) * 100)}%)  Subject: #{topic_post["ygData"]["subject"]}"

      subject = topic_post["ygData"]["subject"].to_s

      if subject.empty?
        topic_title = "No Subject"
      else
        topic_title = CGI.unescapeHTML(subject)[0..399] # PG limits to 400 chars..
      end

      topic_body = CGI.unescapeHTML(topic_post["ygData"]["messageBody"])

      topic = {
        id: tidx + 1,
        user_id: user_id_from_profile(topic_post["ygData"]["profile"]),
        raw: redact_emails(topic_body),
        created_at: Time.at(topic_post["ygData"]["postDate"].to_i),
        cook_method: Post.cook_methods[:raw_html],
        title: topic_title,
        category: CATEGORY_ID,
        custom_fields: { import_id: topic_post["ygData"]["msgId"] }
      }

      topics_count += 1

      # create topic post
      parent_post = create_post(topic, topic[:id])

      # find all posts for topic id
      posts = @collection.find("ygData.topicId": topic_post["ygData"]["topicId"]).to_a

      posts.each_with_index do |p, pidx|

        # skip over first post as this is created by topic above
        next if p["ygData"]["msgId"] == topic_post["ygData"]["topicId"]

        puts "  Post: #{pidx + 1} / #{posts.count()}"

        post_body = CGI.unescapeHTML(p["ygData"]["messageBody"])

        post = {
             id: pidx + 1,
             topic_id: parent_post[:topic_id],
             user_id: user_id_from_profile(p["ygData"]["profile"]),
             raw: redact_emails(post_body),
             created_at: Time.at(p["ygData"]["postDate"].to_i),
             cook_method: Post.cook_methods[:raw_html],
             custom_fields: { import_id: p["ygData"]["msgId"] }
        }

        child_post = create_post(post, post[:id])

        posts_count += 1

      end

    end

    puts "", "Imported #{topics_count} topics with #{topics_count + posts_count} posts."

  end

  # " EAtkin...@...com [vpFREE] <vpF...@...com> wrote:"  
  ### post body might contain PII
  ### remove email addresses as best as we can so that we can show ads
  ### not sure if this is good enough!? for google adsense
  def redact_emails(s)
    (s || "").gsub(/\w{1,3}@(?:\w|\.)*(?=\.[A-Za-z]{2,3}(?:\b))/,"...@..")
  end
  
end

# ImportScripts::YahooGroup.new.perform
