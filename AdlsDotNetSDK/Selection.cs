/// <summary>
/// 
/// </summary>
internal enum Selection
{
    // NOTE: enum name will be used in client side
    ///<summary>Default. Returns everything except policy.</summary>
    Standard,   // default
    ///<summary>Only returns name and type. toOid Won't be honored for Selection.Minimal</summary>
    Minimal,
}