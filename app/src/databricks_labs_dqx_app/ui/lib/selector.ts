const selector = <T>() => ({
  query: {
    select: (data: { data: T }) => data.data,
  },
});

export default selector;
