RSpec.describe "pg_smgrstat extension loading" do
  include_context "pg instance"

  it "starts the server successfully" do
    result = conn.exec("SELECT 1 AS ok")
    expect(result[0]["ok"]).to eq("1")
  end

  it "logs its startup message" do
    expect(pg.log_contents).to include("pg_smgrstat: loaded")
  end
end
