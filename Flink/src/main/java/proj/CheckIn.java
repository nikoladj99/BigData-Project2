package proj;

public class CheckIn {
    public int user;
    public String check_in_time;
    public double latitude;
    public double longitude;
    public String location_id;
    public int time_spent;

    public CheckIn() {}

    public CheckIn(int user, String check_in_time, double latitude, double longitude, String location_id, int time_spent) {
        this.user = user;
        this.check_in_time = check_in_time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.location_id = location_id;
        this.time_spent = time_spent;
    }

    public CheckIn(String location_id, int time_spent) {
        this.location_id = location_id;
        this.time_spent = time_spent;
    }
    

    public String getCheck_in_time(){
        return check_in_time;
    }

    public String getLocation_id() {
        return location_id;
    }

    public void setTime_spent(int time_spent) {
        this.time_spent = time_spent;
    }

    public int getTime_spent() {
        return time_spent;
    }    

    public int getUser() {
        return user;
    }  

    @Override
    public String toString() {
        return "CheckIn{" +
                "user=" + user +
                ", check_in_time='" + check_in_time + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", location_id='" + location_id + '\'' +
                ", time_spent=" + time_spent +
                '}';
    }
}