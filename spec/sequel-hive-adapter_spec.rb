require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "SequelHiveAdapter" do

  before(:all) do
    TEST_HOST = "localhost"
    @testdb = Sequel.connect("hive://#{TEST_HOST}")
    @testtable = 'test_test_test'
  end

  it "should load" do
    @testdb.should be_an_instance_of Sequel::Hive::Database
  end

  it "should convert string columns to strings" do
  end

  it "should handle group and count" do
  end
end
