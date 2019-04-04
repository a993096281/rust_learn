pub mod dbengine;
pub mod log;

pub const LOG_PATH:&'static str = "log.log";

#[derive(Clone)]
pub enum LogOption{
    TypeDelete = 0,
    TypePut = 1,
}

impl ToString for LogOption{
    fn to_string(&self) -> String{
        match self{
            LogOption::TypeDelete => String::from("0"),
            LogOption::TypePut    => String::from("1"),
        }
    }
}