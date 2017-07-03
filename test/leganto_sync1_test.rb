require 'test_helper'

class LegantoSync1Test < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::LegantoSync1::VERSION
  end

  def test_migration
    m = LegantoSync1::Migration.new
    # lists = ['http://lancaster.myreadinglists.org/lists/4510B70F-7C50-D726-4A6C-B129F5EABB2C']
    # lists = ['http://lancaster.rl.talis.com/lists/6F037C3E-D8DE-43D1-36C1-D6DF8F9A2ECA']
    lists = nil
    m.write(ENV['OUTPUT_FILE'], lists)
  end
end
