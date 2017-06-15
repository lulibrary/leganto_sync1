require 'csv'

require 'dotenv/load'
require 'net-ldap'

require 'aspire'
require 'aspire/enumerator/report_enumerator'
require 'aspire/user_lookup'
require 'email_selector'
require 'lusi_api'
require 'redis_hash'

require 'leganto_sync1/version'

module LegantoSync1
  # Helper methods
  module Helpers
    def self.aspire_api_opts(logger = nil, ssl_ca_file = nil, ssl_ca_path = nil)
      {
        logger: logger,
        ssl_ca_file: ssl_ca_file || ENV['ASPIRE_SSL_CA_FILE'],
        ssl_ca_path: ssl_ca_path || ENV['ASPIRE_SSL_CA_PATH']
      }
    end

    def self.aspire_cache(ld_api = nil, json_api = nil, path = nil, mode = nil,
                     logger: nil)
      json_api ||= aspire_json_api(logger: logger)
      ld_api ||= aspire_linked_data_api(logger: logger)
      mode ||= ENV['ASPIRE_CACHE_MODE']
      mode = mode.to_s
      mode = mode.nil? || mode.empty? ? 0o700 : mode.to_i(8)
      path ||= ENV['ASPIRE_CACHE_PATH']
      Aspire::Caching::Cache.new(ld_api, json_api, path,
                                 mode: mode, logger: logger)
    end

    def self.aspire_cache_builder(cache)
      Aspire::Caching::Builder.new(cache)
    end

    def self.aspire_list_enumerator(*periods, list_report: nil)
      list_report ||= ENV['ASPIRE_REPORT_LISTS']
      periods = nil if periods.empty?
      filters = [
        proc { |row| periods.include?(row['Time Period']) },
        proc { |row| row['Status'].to_s.start_with?('Published') },
        proc { |row| row['Privacy Control'] == 'Public' }
      ]
      Aspire::Enumerator::ReportEnumerator.new(list_report, filters)
                                          .enumerator
    end

    def self.aspire_object_factory(cache = nil, user_lookup = nil,
                                   logger: nil, redis: nil)
      cache ||= aspire_cache(logger: logger)
      user_lookup ||= aspire_user_lookup(redis: redis)
      Aspire::Object::Factory.new(cache, user_lookup)
    end

    def self.aspire_json_api(client_id = nil, secret = nil, tenant = nil,
                        logger: nil)
      client_id ||= ENV['ASPIRE_API_CLIENT_ID']
      secret ||= ENV['ASPIRE_API_SECRET']
      tenant ||= ENV['ASPIRE_TENANT']
      Aspire::API::JSON.new(client_id, secret, tenant,
                            **aspire_api_opts(logger))
    end

    def self.aspire_linked_data_api(tenant = nil, linked_data_root = nil,
                                    tenancy_root = nil, aliases = nil,
                                    logger: nil)
      aliases ||= ENV['ASPIRE_TENANCY_HOST_ALIASES'].to_s.split(';')
      linked_data_root ||= ENV['ASPIRE_LINKED_DATA_ROOT']
      tenant ||= ENV['ASPIRE_TENANT']
      tenancy_root ||= ENV['ASPIRE_TENANCY_ROOT']
      Aspire::API::LinkedData.new(tenant,
                                  linked_data_root: linked_data_root,
                                  tenancy_host_aliases: aliases,
                                  tenancy_root: tenancy_root,
                                  **aspire_api_opts(logger))
    end

    def self.aspire_user_lookup(user_report = nil, store = nil, redis: nil)
      user_report ||= ENV['ASPIRE_REPORT_USERS']
      unless store
        redis ||= redis_api
        store ||= redis_hash('aspire:user:', redis: redis)
      end
      Aspire::UserLookup.new(filename: user_report, store: store)
    end

    def self.courses(course_file = nil)
      # Build a mapping of { module code => { year => [course-codes] } }
      course_codes = {}
      course_file ||= ENV['COURSE_FILE']
      CSV.foreach(course_file, {col_sep: "\t"}) do |row|
        prefix, module_code, year, cohort = row[0].split('-')
        # Course fields:
        # [code, section-id, search-id1, search-id2, search-id3, mnemonic]
        course_fields = [row[0], row[2], row[14], row[15], row[16], row[14]]
        course_id = "M#{module_code}"
        course_year = row[13].to_i
        course_codes[course_id] = {} unless course_codes.has_key?(course_id)
        course_code = course_codes[course_id]
        if course_code[course_year]
          course_code[course_year].push(course_fields)
        else
          course_code[course_year] = [course_fields]
        end
      end
      # Add an unknown definition for when a list has no modules
      course_codes[:unknown] = ['unknown', '1', '', '', '', 'UNKNOWN']
      course_codes
    end

    def self.email_selector(config = nil, map = nil, store = nil, redis: nil)
      config ||= ENV['EMAIL_SELECTOR_CONFIG']
      map ||= ENV['EMAIL_SELECTOR_MAP']
      unless store
        redis ||= redis_api
        store ||= redis_hash(namespace: 'email:', redis: redis)
      end
      begin
        EmailSelector::Selector.new(config: config, map: map, store: store)
      rescue StandardError => e
        puts e
      end
    end

    def self.env(key, default = nil, conv: nil)
      return default if ENV[key].nil? || ENV[key].empty?
      conv ? conv.call(ENV[key]) : ENV[key]
    end

    def self.ldap_lookup(host = nil, user = nil, password = nil, base = nil)
      base ||= ENV['LDAP_BASE']
      host ||= ENV['LDAP_HOST']
      password ||= ENV['LDAP_PASSWORD']
      user ||= ENV['LDAP_USER']
      LDAPLookup.new(host, user, password, base)
    end

    def self.logger(log_file = nil)
      log_file ||= ENV['ASPIRE_LOG']
      logger = Logger.new("| tee #{log_file}") # @log_file || STDOUT)
      logger.datetime_format = '%Y-%m-%d %H:%M:%S'
      logger.formatter = proc do |severity, datetime, _program, msg|
        "#{datetime} [#{severity}]: #{msg}\n"
      end
      logger
    end

    def self.redis_api(flush = false, db: nil, host: nil, port: nil, url: nil)
      db ||= env('REDIS_DB', conv: proc(&:to_i))
      host ||= env('REDIS_HOST', 'localhost')
      port ||= env('REDIS_PORT', conv: proc(&:to_i))
      url ||= ENV['REDIS_URL']
      redis = if url.nil? || url.empty?
                Redis.new(db: db, host: host, port: port)
              else
                Redis.new(url: url)
              end
      redis.flushdb if flush
      redis
    end

    def self.redis_hash(namespace, flush: false, redis: nil, **redis_args)
      redis ||= redis_api(flush, **redis_args)
      RedisHash::Hash.new(namespace: namespace, redis: redis)
    end
  end

  # Retrieves user details from LDAP directory
  class LDAPLookup
    # @!attribute [rw] base
    #   @return [String] the root of the LDAP user tree
    attr_accessor :base

    # @!attribute [rw] cache
    #   @return [Hash<String, String>] cached email => user ID lookups
    attr_accessor :cache

    # @!attribute [rw] use_cache
    #   @return [Boolean] if true, cache LDAP responses and use for subsequent searched
    attr_accessor :use_cache

    # Initialises a new LDAPLookup instance
    # @see (LegantoSync::ReadingLists::Aspire::LDAPLookup#open)
    # @return [void]
    def initialize(host, user, password, base, use_cache: false)
      self.base = base
      self.cache = {}
      self.use_cache = use_cache
      open(host, user, password)
    end

    # Clears the cache
    # @return [void]
    def clear
      cache.clear
    end

    # Closes the LDAP connection
    # @return [void]
    def close
      @ldap.close if @ldap
      @ldap = nil
    end

    # Returns the username of the user matching the supplied email address
    # @param email [String] the user's email address
    def find(email = nil)
      # Search cache
      if use_cache
        uid = cache[email]
        return uid if uid
      end

      # Search LDAP for the email address as given
      filter = Net::LDAP::Filter.eq('mail', email)
      @ldap.search(attributes: ['uid'], base: base, filter: filter) do |entry|
        uid = get_uid(email, entry)
        return uid if uid
      end

      # The exact email address wasn't found, try the form "username@domain" if the username component looks like
      # a username (assumes that usernames do not contain punctuation)
      user, domain = email.split('@')
      unless user.nil? || user.empty? || user.include?('.')
        filter = Net::LDAP::Filter.eq('uid', user)
        @ldap.search(attributes: ['uid'], base: base, filter: filter) do |entry|
          uid = get_uid(email, entry)
          return uid if uid
        end
      end

      # No matches found
      nil
    end

    # Opens the LDAP connection
    # @param host [String] the LDAP server
    # @param user [String] the LDAP bind username
    # @param password [String] the LDAP bind password
    # @return [void]
    def open(host, user = nil, password = nil)
      @ldap = Net::LDAP.new
      @ldap.host = host
      @ldap.port = 389
      @ldap.auth(user, password)
      @ldap.bind
    end

    private

    # Returns the 'uid' property from an LDAP entry
    # @param email [String] the email address corresponding to the LDAP entry
    # @param ldap_entry [Net::LDAP::Entry] the LDAP entry
    # @return [String] the first value of the 'uid' property
    def get_uid(email, ldap_entry)
      # Get the first uid value (this should always be the canonical username)
      uid = ldap_entry.uid ? ldap_entry.uid[0] : nil
      # Update the cache
      cache[email] = uid if uid && use_cache
      # Return the uid
      uid
    end
  end

  # Generates Leganto import file from Aspire API data
  class Migration
    attr_accessor :courses
    attr_accessor :email_selector
    attr_accessor :ldap_lookup
    attr_accessor :logger
    attr_accessor :lists
    attr_accessor :object_factory
    attr_accessor :redis

    def initialize
      self.courses = Helpers.courses
      self.ldap_lookup = Helpers.ldap_lookup
      self.logger = Helpers.logger
      self.redis = Helpers.redis_api
      self.email_selector = Helpers.email_selector(redis: redis)
      self.lists = Helpers.aspire_list_enumerator('2016-17', '2015-16')
      self.object_factory = Helpers.aspire_object_factory(logger: logger,
                                                          redis: redis)
    end

    def write(filename, lists = nil)
      lists ||= self.lists
      writer = Writer.new(courses: courses,
                          email_selector: email_selector,
                          factory: object_factory,
                          ldap_lookup: ldap_lookup,
                          logger: logger)
      writer.write(filename, lists)
    end
  end

  class Writer
    attr_accessor :courses
    attr_accessor :email_selector
    attr_accessor :factory
    attr_accessor :filename
    attr_accessor :ldap_lookup
    attr_accessor :logger

    def initialize(courses: nil, email_selector: nil, factory: nil,
                   ldap_lookup: nil, logger: nil)
      self.courses = courses
      self.email_selector = email_selector
      self.factory = factory
      self.ldap_lookup = ldap_lookup
      self.logger = logger
    end

    def header
      row = []
      row[0] = 'course_code'
      row[1] = 'Section id'
      row[2] = 'Searchable id1'
      row[3] = 'Searchable id2'
      row[4] = 'Searchable id3'
      row[5] = 'Reading_list_code'
      row[6] = 'Reading list name'
      row[7] = 'Reading List Description'
      row[8] = 'Reading lists Status'
      row[9] = 'RLStatus'
      row[10] = 'visibility'
      row[11] = 'owner_user_name'
      row[12] = 'section_name'
      row[13] = 'section_description'
      row[14] = 'section_start_date'
      row[15] = 'section_end_date'
      row[16] = 'citation_secondary_type'
      row[17] = 'citation_status'
      row[18] = 'citation_tags'
      row[19] = 'citation_originating_system_id'
      row[20] = 'citation_title'
      row[21] = 'citation_journal_title'
      row[22] = 'citation_author'
      row[23] = 'citation_publication_date'
      row[24] = 'citation_edition'
      row[25] = 'citation_isbn'
      row[26] = 'citation_issn'
      row[27] = 'citation_place_of_publication'
      row[28] = 'citation_publisher'
      row[29] = 'citation_volume'
      row[30] = 'citation_issue'
      row[31] = 'citation_pages'
      row[32] = 'citation_start_page'
      row[33] = 'citation_end_page'
      row[34] = 'citation_doi'
      row[35] = 'citation_chapter'
      row[36] = 'citation_source'
      row[37] = 'citation_note'
      row[38] = 'additional_person_name'
      row[39] = 'citation_public_note'
      row[40] = 'external_system_id'
      row
    end

    def row(list = nil, item = nil, course_code = nil)
      list_status = 'BeingPrepared'
      list_visibility = 'RESTRICTED'
      rl_status = 'DRAFT'
      rl_code = if course_code[5] == 'UNKNOWN' || list.time_period.nil?
                  File.basename(list.uri)
                else
                  "#{course_code[5]}_#{list.time_period.year}"
                end
      # Concatenate nested sections into a single section name
      # item.parent_sections returns sections in nearest-furthest order, but we
      # want to concatenate in furthest-nearest order, so we reverse the list
      sections = item.parent_sections
      section_description = ''
      section_end_date = ''
      section_name = sections.reverse.join(' - ')
      section_start_date = ''
      # Take the section description from the nearest enclosing section with a
      # description
      sections.each do |section|
        if section.description
          section_description = section.description
          break
        end
      end

      citation_status = 'BeingPrepared'

      resource = item.resource

      row = []
      # course_code
      row[0] = course_code[0]
      # Section id
      row[1] = course_code[1]
      # Searchable id1
      row[2] = course_code[2]
      # Searchable id2
      row[3] = course_code[3]
      # Searchable id3
      row[4] = course_code[4]
      # Reading_list_code
      row[5] = rl_code
      # Reading list name
      row[6] = list.name
      # Reading List Description
      row[7] = list.description
      # Reading lists Status
      row[8] = list_status
      # RLStatus
      row[9] = rl_status
      # visibility
      row[10] = list_visibility
      # owner_user_name
      row[11] = list_owner_username(list)
      # section_name
      row[12] = section_name
      # section_description
      row[13] = section_description
      # section_start_date
      row[14] = section_start_date
      # section_end_date
      row[15] = section_end_date
      # citation_secondary_type
      row[16] = citation_type(resource)
      # citation_status
      row[17] = citation_status
      # citation_tags
      row[18] = citation_tags(item)
      if resource
        # citation_originating_system_id
        row[19] = resource.citation_local_control_number
        # citation_title
        row[20] = resource.citation_title || item.title
        # citation_journal_title
        row[21] = resource.journal_title
        # citation_author
        row[22] = citation_authors(resource)
        # citation_publication_date
        row[23] = resource.citation_date
        # citation_edition
        row[24] = resource.citation_edition
        # citation_isbn
        row[25] = resource.citation_isbn10 || resource.citation_isbn13
        # citation_issn
        row[26] = resource.citation_issn
        # citation_place_of_publication
        row[27] = resource.citation_place_of_publication
        # citation_publisher
        row[28] = resource.citation_publisher
        # citation_volume
        row[29] = resource.citation_volume
        # citation_issue
        row[30] = resource.citation_issue
        # citation_pages
        row[31] = resource.citation_page
        # citation_start_page
        row[32] = resource.citation_page_start
        # citation_end_page
        row[33] = resource.citation_page_end
        # citation_doi
        row[34] = resource.citation_doi
        # citation_chapter
        row[35] = resource.chapter_title
        # citation_source
        row[36] = resource.citation_url
      else
        row[19] = item.local_control_number
        row[20] = item.title
        (21..36).each { |i| row[i] = '' }
      end

      # citation_start_page
      row[32] = ''
      # citation_end_page
      row[33] = ''
      # citation_note
      row[37] = item.library_note
      # additional_person_name
      row[38] = '' # TODO: What's the use case for this?
      # citation_public_note
      row[39] = item.student_note
      # external_system_id
      row[40] = '' # TODO: What's the correct value for this?

      # Return the row
      row
    end

    def write(filename, lists)
      CSV.open(filename, 'wb', force_quotes: true) do |file|
        file << header
        lists.each do |list|
          list = list[1] unless list.is_a?(String)
          list = factory.get(list)
          write_list(file, list)
        end
      end
    end

    # Writes a list to the file
    # @param file [IO] an opened file
    # @param list [Aspire::Object::List] the list to write
    # @param all [Boolean] if true, write all list items, otherwise write only
    #   list items which have resources
    # @return [void]
    def write_list(file, list, all: false)
      modules = list.modules ? list.modules.compact : []
      if modules.empty?
        logger.debug("#{list.uri}: course unknown (no modules)") if logger
        write_list_entries(file, list, courses[:unknown], all: all)
      else
        modules.each { |mod| write_list_module(file, list, mod, all: all) }
      end
    end

    private

    def citation_authors(resource)
      authors = resource.authors
      if authors.nil?
        ''
      elsif authors.is_a?(Array)
        authors.join('; ')
      else
        authors.to_s
      end
    end

    def citation_tags(item)
      tags = []
      # Infer tag from importance
      importance = item.importance ? item.importance.downcase : nil
      case importance
      when 'essential'
        tags.push('ESS')
      when 'optional'
        tags.push('OPT')
      when 'recommended'
        tags.push('REC')
      when 'suggested for student purchase'
        tags.push('SSP')
      end
      # May want to infer tag from enclosing sections
      tags.join(',')
    end

    def citation_type(resource)
      result = resource && resource.type ? resource.type.split('/')[-1] : nil
      if result
        result.delete!(' ')
        result.upcase!
      end
      result
    end

    def list_owner_username(list)
      return '' if email_selector.nil? || ldap_lookup.nil?
      owner = list.owner[0] || list.creator[0]
      return '' if owner.nil?
      # Get the primary email address from the list owner's email address list
      email = email_selector.email(owner.email || [])
      # Get the username from the primary email address
      return '' if email.nil? || email.empty?
      ldap_lookup.find(email) || ''
    end

    # Writes a list to the file for a specific module
    # @param file [IO] an opened file
    # @param list [Aspire::Object::List] the list to write
    # @param mod [Aspire::Object::Module] the course module for the list
    # @param all [Boolean] if true, write all list items, otherwise write only
    def write_list_module(file, list, mod, all: false)
      # Get the course codes applicable to this module/year (i.e. all cohorts
      # for the module/year)
      course_unknown = 'course unknown'
      if list.time_period.nil?
        codes = nil
        course_unknown += ', no time period'
      else
        year = list.time_period.year
        codes = courses[mod.code]
        codes = codes ? codes[year] : nil
      end
      # return if course_codes.nil? || course_codes.empty?
      if codes.nil? || codes.empty?
        logger.debug("#{list.uri}: #{course_unknown} (#{mod.code})") if logger
        codes = [courses[:unknown]]
      end
      codes.each { |code| write_list_entries(file, list, code, all: all) }
    end

    # Writes a list for a specific course code to the file
    # @param file [IO] an opened file
    # @param list [Aspire::Object::List] the list to write
    # @param course_code [String] the course code for the list
    # @param all [Boolean] if true, write all list items, otherwise write only
    # @return [void]
    def write_list_entries(file, list, course_code, all: false)
      list.each_item do |item|
        file << row(list, item, course_code) if all || item.resource
      end
    end
  end
end