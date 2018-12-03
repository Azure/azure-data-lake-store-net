using System;

namespace Microsoft.Azure.DataLake.Store.QueueTools
{
    /// <summary>
    /// Priority Queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class PriorityQueue<T> where T:IComparable
    {
        private const int InitialCapacity = 102400;
        private int _capacity;
        internal int Capacity{
            get
            {
                    return _capacity;
            }
        }
        private const int GrowFactor=2;
        private T[] _heap;
        private int _heapSize;
        internal int HeapSize {
            get {
                 return _heapSize;
            }
        }

        private int Parent(int index)
        {
            return (index - 1) / 2;
        }

        private int Left(int index)
        {
            return 2 * index + 1;
        }

        private int Right(int index)
        {
            return 2 * index + 2;
        }
        internal PriorityQueue()
        {
            _heap=new T[InitialCapacity];
            _capacity = InitialCapacity;
            _heapSize = 0;
        }

        internal PriorityQueue(int capacity)
        {
            _heap=new T[capacity];
            _capacity = capacity;
            _heapSize = 0;
        }

        private void Grow()
        {
            _capacity = _capacity * GrowFactor;
            T[] newHeap=new T[_capacity];
            Array.Copy(_heap,newHeap,_heap.Length);
            _heap = newHeap;
        }
        internal void Add(T elem)
        {
            if (_heapSize >= _capacity)
            {
                Grow();
            }
            _heap[_heapSize++] = elem;
            Bubble();
        }

        internal T SeekMax()
        {
            return _heap[0];
        }
        internal T GetMax()
        {
            T ret;
            ret = _heap[0];
            _heap[0] = _heap[--_heapSize];
            MaxHeapify(0);
            return ret;
        }

        private void MaxHeapify(int index)
        {
            T largest=_heap[index];
            int largestIndex = index;
            int left = Left(index);
            int right = Right(index);
            if (left < _heapSize && _heap[left] .CompareTo(largest)>0)
            {
                largestIndex = left;
                largest = _heap[left];
            }
            if (right< _heapSize && _heap[right].CompareTo(largest) > 0)
            {
                largestIndex = right;
            }
            if (largestIndex != index)
            {
                Swap(index, largestIndex);
                MaxHeapify(largestIndex);
            }
        }
        private void Bubble()
        {
            int i = _heapSize - 1;
            int parent = Parent(i);
            while (i > 0 && _heap[i].CompareTo(_heap[parent]) > 0)
            {
                Swap(i, parent);
                i = parent;
                parent = Parent(i);
            }
        }

        private void Swap(int ind1, int ind2)
        {
             T temp = _heap[ind1];
            _heap[ind1] = _heap[ind2];
            _heap[ind2] = temp;
        }

    }
}