use crate::error::Result;

pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
}

impl<T> Transformed<T> {
    pub fn yes(data: T) -> Self {
        Transformed {
            data,
            transformed: true,
        }
    }

    pub fn no(data: T) -> Self {
        Transformed {
            data,
            transformed: false,
        }
    }

    pub fn map_data<U, F>(self, f: F) -> Result<Transformed<U>>
    where
        F: FnOnce(T) -> Result<U>,
    {
        f(self.data).map(|u| Transformed {
            data: u,
            transformed: self.transformed,
        })
    }

    pub fn update<U, F>(self, f: F) -> Transformed<U>
    where
        F: FnOnce(T) -> U,
    {
        Transformed {
            data: f(self.data),
            transformed: self.transformed,
        }
    }

    pub fn transform_children<F>(self, f: F) -> Result<Transformed<T>>
    where
        F: FnOnce(T) -> Result<Transformed<T>>,
    {
        f(self.data).map(|mut t| {
            t.transformed |= self.transformed;
            t
        })
    }
}

pub trait TransformedResult<T> {
    fn data(self) -> Result<T>;
}

impl<T> TransformedResult<T> for Result<Transformed<T>> {
    fn data(self) -> Result<T> {
        self.map(|t| t.data)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TreeNodeRecursion {
    Continue,
    Stop,
}

impl TreeNodeRecursion {
    pub fn visit_children<F>(self, f: F) -> Result<TreeNodeRecursion>
    where
        F: FnOnce() -> Result<TreeNodeRecursion>,
    {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

pub trait TransformNode: Sized + Clone {
    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>;

    fn apply_children<'n, F>(&'n self, f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'n Self) -> Result<TreeNodeRecursion>;

    /// Deprecated, use transform_down instead
    /// TODO: remove this method in the future
    fn transform<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        transform_down_impl(self, &mut f)
    }

    fn transform_up<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        transform_up_impl(self, &mut f)
    }

    fn transform_down<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        transform_down_impl(self, &mut f)
    }

    fn apply<'n, F>(&'n self, mut f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'n Self) -> Result<TreeNodeRecursion>,
    {
        apply_impl(self, &mut f)
    }
}

fn transform_down_impl<N, F>(node: N, f: &mut F) -> Result<Transformed<N>>
where
    N: TransformNode,
    F: FnMut(N) -> Result<Transformed<N>>,
{
    f(node)?.transform_children(|n| n.map_children(|c| transform_down_impl(c, f)))
}

fn transform_up_impl<N, F>(node: N, f: &mut F) -> Result<Transformed<N>>
where
    N: TransformNode,
    F: FnMut(N) -> Result<Transformed<N>>,
{
    node.map_children(|c| transform_up_impl(c, f))?.transform_children(f)
}

fn apply_impl<'n, N: TransformNode, F: FnMut(&'n N) -> Result<TreeNodeRecursion>>(
    node: &'n N,
    f: &mut F,
) -> Result<TreeNodeRecursion> {
    f(node)?.visit_children(|| node.apply_children(|c| apply_impl(c, f)))
}

pub trait TreeNodeContainer<'a, T: 'a> {
    fn apply<F>(&'a self, f: F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&'a T) -> Result<TreeNodeRecursion>;
}
