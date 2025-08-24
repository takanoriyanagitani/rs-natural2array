pub use arrow;

use arrow::array::ArrowNativeTypeOp;

use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;

use arrow::datatypes::ArrowPrimitiveType;

pub fn is_negative<N>(natural: N) -> bool
where
    N: ArrowNativeTypeOp,
{
    let zero: N = N::ZERO;
    natural < zero
}

pub fn negative2none<N>(natural: N) -> Option<N>
where
    N: ArrowNativeTypeOp,
{
    let neg: bool = is_negative(natural);
    let pos: bool = !neg;
    pos.then_some(natural)
}

pub fn natural2builder<T>(natural: T::Native, bldr: &mut PrimitiveBuilder<T>)
where
    T: ArrowPrimitiveType,
{
    let o: Option<T::Native> = negative2none(natural);
    match o {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub fn opt2builder<T>(natural: Option<T::Native>, bldr: &mut PrimitiveBuilder<T>)
where
    T: ArrowPrimitiveType,
{
    let o: Option<T::Native> = natural.and_then(negative2none);
    match o {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub fn natural2array<I, T>(nat: I, cap: usize) -> PrimitiveArray<T>
where
    I: Iterator<Item = T::Native>,
    T: ArrowPrimitiveType,
{
    let mut bldr = PrimitiveBuilder::with_capacity(cap);
    for n in nat {
        natural2builder(n, &mut bldr);
    }
    bldr.finish()
}

pub fn opts2array<I, T>(nat: I, cap: usize) -> PrimitiveArray<T>
where
    I: Iterator<Item = Option<T::Native>>,
    T: ArrowPrimitiveType,
{
    let mut bldr = PrimitiveBuilder::with_capacity(cap);
    for o in nat {
        opt2builder(o, &mut bldr);
    }
    bldr.finish()
}

pub const CAPACITY_DEFAULT: usize = 1024;

pub fn natural2array_default<I, T>(nat: I) -> PrimitiveArray<T>
where
    I: Iterator<Item = T::Native>,
    T: ArrowPrimitiveType,
{
    natural2array(nat, CAPACITY_DEFAULT)
}

pub fn opts2array_default<I, T>(nat: I) -> PrimitiveArray<T>
where
    I: Iterator<Item = Option<T::Native>>,
    T: ArrowPrimitiveType,
{
    opts2array(nat, CAPACITY_DEFAULT)
}

macro_rules! nat2arr {
    ($fname: ident, $ptyp: ty) => {
        /// Converts the natural numbers to an array.
        pub fn $fname<I>(nat: I) -> PrimitiveArray<$ptyp>
        where
            I: Iterator<Item = <$ptyp as ArrowPrimitiveType>::Native>,
        {
            natural2array_default(nat)
        }
    };
}

nat2arr!(nat2arr8i, arrow::datatypes::Int8Type);
nat2arr!(nat2arr16i, arrow::datatypes::Int16Type);
nat2arr!(nat2arr32i, arrow::datatypes::Int32Type);
nat2arr!(nat2arr64i, arrow::datatypes::Int64Type);

macro_rules! opt2arr {
    ($fname: ident, $ptyp: ty) => {
        /// Converts the optionals to an array.
        pub fn $fname<I>(nat: I) -> PrimitiveArray<$ptyp>
        where
            I: Iterator<Item = Option<<$ptyp as ArrowPrimitiveType>::Native>>,
        {
            opts2array_default(nat)
        }
    };
}

opt2arr!(opt2arr8i, arrow::datatypes::Int8Type);
opt2arr!(opt2arr16i, arrow::datatypes::Int16Type);
opt2arr!(opt2arr32i, arrow::datatypes::Int32Type);
opt2arr!(opt2arr64i, arrow::datatypes::Int64Type);

pub fn num2opt(number: &serde_json::Number) -> Option<i64> {
    let oi: Option<i64> = number.as_i64();
    oi.and_then(negative2none)
}

pub fn val2opt(val: &serde_json::Value) -> Option<i64> {
    match val {
        serde_json::Value::Number(n) => num2opt(n),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    mod test_nat2arr {
        mod test_short {
            use crate::nat2arr16i;

            use arrow::array::Array;
            use arrow::array::PrimitiveArray;

            #[test]
            fn test_multi() {
                let v: Vec<i16> = vec![0, 42, -1, 634, -42, 333];
                let pa: PrimitiveArray<_> = nat2arr16i(v.into_iter());
                assert_eq!(6, pa.len());
                assert_eq!(2, pa.null_count());
                assert_eq!(pa.values(), &[0, 42, 0, 634, 0, 333]);
            }
        }
    }

    mod test_opt2arr {
        mod test_short {
            use crate::opt2arr16i;

            use arrow::array::Array;
            use arrow::array::PrimitiveArray;

            #[test]
            fn test_multi() {
                let v: Vec<Option<i16>> = vec![
                    Some(0),
                    Some(42),
                    Some(-1),
                    Some(634),
                    None,
                    Some(-42),
                    Some(333),
                ];
                let pa: PrimitiveArray<_> = opt2arr16i(v.into_iter());
                assert_eq!(7, pa.len());
                assert_eq!(3, pa.null_count());
                assert_eq!(pa.values(), &[0, 42, 0, 634, 0, 0, 333]);
            }
        }
    }
}
