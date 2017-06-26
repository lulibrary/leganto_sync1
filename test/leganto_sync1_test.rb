require 'test_helper'

class LegantoSync1Test < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::LegantoSync1::VERSION
  end

  def test_migration
    m = LegantoSync1::Migration.new
    lists = ['http://lancaster.myreadinglists.org/lists/4510B70F-7C50-D726-4A6C-B129F5EABB2C']
    m.write(ENV['OUTPUT_FILE'])
  end
end
