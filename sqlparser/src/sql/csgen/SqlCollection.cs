using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;




using RSSBus.core;
using RSSBus;
using System.Collections.Generic;
namespace CData.Sql {




public class SqlCollection<T> : IEnumerable<T>, ICollection<T>, IList<T>, IList, ICollection, IEnumerable, ISqlCloneable
{
  private List<T> data = new List<T>();
  public static implicit operator SqlCollection<T>(List<T> x)
  {
      SqlCollection<T> newVAr = new SqlCollection<T>();
      newVAr.SetBaseList(x);
      return newVAr;
 }

  public static implicit operator List<T>(SqlCollection<T> x)
  {
      return x.GetBaseList();
  }

  public void Add(T arg)
  {
      this.data.Add(arg);
  }

  public void Add(int i, T arg)
  {
      this.data.Insert(i, arg);
  }

  public void AddAll(ICollection<T> collection){
      this.data.AddRange(collection);
  }

  public  T Get(int i)
  {
    return (T)this[i];
  }

  public void Set(int i, T arg)
  {
      this[i] = arg;
  }

  public Object Clone() {
    SqlCollection<T> cloned = new SqlCollection<T>();
    foreach (T t in this) {
      Object obj = (Object)t;
      if (obj is ISqlCloneable) {
        cloned.Add((T)((ISqlCloneable)obj).Clone());
      } else {
        cloned.Add(t);
      }
    }
    return cloned;
  }

  IEnumerator IEnumerable.GetEnumerator()
  {
      return (IEnumerator)GetEnumerator();
  }

  IEnumerator<T> IEnumerable<T>.GetEnumerator()
  {
      return (IEnumerator<T>)GetEnumerator();
  }

  public SqlCollectionEnum<T> GetEnumerator()
  {
      return new SqlCollectionEnum<T>(this);
  }

  public void SetBaseList(List<T> mydata)
  {
      this.data = mydata;
  }

  public List<T> GetBaseList()
  {
      return data;
  }

  public T this[int index]
  {
      get
      {
          return (T)data[index];
      }
      set
      {
          data[index] = value;
      }
  }

  public int Size()
  {
      return data.Count;
  }

  public T[] ToArray(Type type)
  {
      T[] ret = new T[Size()];
      for (int i = 0; i < Size(); i++)
      {
          ret[i] = Get(i);
      }
      return ret;
  }
  public bool Contains(T item)
  {
      return data.Contains(item);
  }

  public virtual void Clear()
  {
      data.Clear();
  }

  public int IndexOf(T item)
  {
      return data.IndexOf(item);
  }

  public int Count { get { return Size(); } }

  public void RemoveAt(int index)
  {
    data.RemoveAt(index);
  }

  public void CopyTo(T[] array, int arrayIndex){
    data.CopyTo(array, arrayIndex);
  }

  bool IList.IsReadOnly {
    get { return ((IList)data).IsReadOnly; }
  }

  bool IList.IsFixedSize{
    get { return ((IList) data).IsFixedSize; }
  }

  object IList.this[int index]{
    get { return ((IList) data)[index]; }
    set { ((IList) data)[index] = value; }
  }

  void IList.Remove(object item){
    ((IList) data).Remove(item);
  }

  void IList.Insert(int index, object item){
    ((IList) data).Insert(index, item);
  }

  int IList.IndexOf(object item){
    return ((IList) data).IndexOf(item);
  }

  bool IList.Contains(object item){
    return ((IList) data).Contains(item);
  }

  int IList.Add(object item){
    return ((IList) data).Add(item);
  }

  void ICollection.CopyTo(Array array, int arrayIndex){
    ((ICollection) data).CopyTo(array, arrayIndex);
  }

  bool ICollection.IsSynchronized{
    get { return ((ICollection) data).IsSynchronized; }
  }

  object ICollection.SyncRoot{
    get { return ((ICollection) data).SyncRoot; }
  }

  bool ICollection<T>.IsReadOnly{
    get { return ((ICollection<T>) data).IsReadOnly; }
  }

  public void Insert(int index, T item){
    data.Insert(index, item);
  }

  public bool Remove(T item) {
    return data.Remove(item);
  }

  public void Sort(Comparison<T> comparison) {
    data.Sort(comparison);
  }
}

 public class SqlCollectionEnum<T> : IEnumerator<T>
  {
      private SqlCollection<T> _list;
      private int curIndex;
      private T cur;

      public SqlCollectionEnum(SqlCollection<T> item)
      {
          _list = item;
          curIndex = -1;
          cur = default(T);
      }

      public bool MoveNext()
      {
          if (++curIndex >= _list.Size())
          {
              return false;
          }
          else
          {
              cur = _list[curIndex];
          }
          return true;
      }

      public void Reset() { curIndex = -1; }
      void IDisposable.Dispose() { }
      public T Current
      {
          get { return cur; }
      }
      object IEnumerator.Current
      {
          get { return Current; }
      }
  }


}

