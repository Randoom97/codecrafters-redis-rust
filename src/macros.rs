macro_rules! option_get_or_return_none {
    ($variable:ident, $option_method:expr) => {
        let variable_option = $option_method;
        if variable_option.is_none() {
            return None;
        }
        let $variable = variable_option.unwrap();
    };
}
pub(crate) use option_get_or_return_none;

macro_rules! result_get_or_return_none {
    ($variable:ident, $result_method:expr) => {
        let variable_result = $result_method;
        if variable_result.is_err() {
            return None;
        }
        let $variable = variable_result.unwrap();
    };
}
pub(crate) use result_get_or_return_none;

macro_rules! option_type_guard {
    ($variable_option:ident, $match:expr, $type_to_check:path) => {
        let $variable_option = match $match {
            $type_to_check(variable) => Some(variable),
            _ => None,
        };
    };
}
pub(crate) use option_type_guard;
